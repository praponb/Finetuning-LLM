from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance_scaled", "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("customer_id").orderBy("balance_scaled_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("running_min_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-04-01").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.round((F.col("balance") * 1.07), 2).alias("balance_scaled"),
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= "2023-11-01"), "left_anti")
)

with_windows = (
    eligible
    .withColumn("balance_scaled_bucket", F.ntile(5).over(w))
    .withColumn("balance_bucket", F.ntile(10).over(w2))
)

with_windows_accounts = (
    with_windows
    .filter(F.col("balance_scaled") <= 41)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "account_type_title",
        "balance_scaled_bucket",
        F.max("balance").over(w3).alias("running_max_balance"),
        F.sum("balance").over(w3).alias("running_sum_balance"),
        F.avg("balance").over(w3).alias("running_avg_balance"),
        F.min("balance").over(w3).alias("running_min_balance"),
    )
)

not_flagged = (
    with_windows_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant").isin(["Amazon", "Starbucks", "7-Eleven"])), "left_anti")
)

joined = (
    not_flagged
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.account_type_title",
        "a.balance_scaled_bucket",
        "a.running_max_balance",
        "a.running_avg_balance",
        "a.running_min_balance",
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.col("c.age"),
    )
)

with_windows_accounts_2 = (
    joined
    .withColumn("running_max_credit_limit", F.max("credit_limit").over(w4))
    .withColumn("running_min_credit_limit", F.min("credit_limit").over(w4))
    .withColumn("running_avg_credit_limit", F.avg("credit_limit").over(w4))
)

result = (
    with_windows_accounts_2
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["country", "account_city"], ["country"]],
        F.col("dim.country").alias("country"),
        F.col("e.city").alias("account_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.running_avg_balance")).alias("min_running_avg_balance"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("account_city").desc_nulls_first())
)
