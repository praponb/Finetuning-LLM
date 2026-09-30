from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w_hist = Window.orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy(F.month("opened_date"), "customer_id").orderBy("running_min_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("running_avg_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w9 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_accounts
    .select(
        "*",
        F.sum("balance").over(w).alias("account_type_sum_balance"),
        F.ntile(4).over(w_hist).alias("balance_bucket"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.account_type",
        "a.opened_date",
        F.col("p.signup_date"),
        F.col("p.email"),
        F.col("p.credit_limit"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.min("credit_limit").over(w3).alias("running_min_credit_limit"),
        F.avg("credit_limit").over(w3).alias("running_avg_credit_limit"),
        F.max("credit_limit").over(w3).alias("running_max_credit_limit"),
    )
)

enriched = (
    with_windows
    .filter(F.col("customer_id").isin([11, 16, 36]))
    .select(
        "id",
        "opened_date",
        "running_min_credit_limit",
        "running_avg_credit_limit",
        "running_max_credit_limit",
        F.avg("running_avg_credit_limit").over(w4).alias("running_avg_credit_limit_avg_5"),
        F.sum("running_min_credit_limit").over(w5).alias("cumulative_running_min_credit_limit"),
        F.sum("running_avg_credit_limit").over(w6).alias("cumulative_running_avg_credit_limit"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.running_min_credit_limit",
        "s.running_max_credit_limit",
        "s.cumulative_running_min_credit_limit",
        F.col("c.txn_time"),
        F.col("c.txn_type"),
    )
)

with_gaps = (
    latest_match
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w8).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w8) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "running_max_credit_limit",
        "txn_time",
        "txn_type",
        F.sum("session_start_flag").over(w9).alias("session_id"),
    )
)

with_latest = (
    sessionized
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.merchant"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "running_max_credit_limit",
        "txn_time",
        "txn_type",
        "session_id",
        "transaction_txn_type",
        F.when(F.col("txn_type").rlike("^(FEE|CREDIT)$"), "CORE").otherwise("GROUP_B").alias("txn_type_zone"),
    )
    .orderBy(F.col("transaction_txn_type").desc_nulls_last())
)
