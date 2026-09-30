from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.date_trunc("WEEK", "signup_date")).orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("signup_date")).orderBy("balance", "id")
w5 = Window.partitionBy("account_type", "country").orderBy("signup_date", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("txn_type").orderBy("running_balance", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("prev_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

with_dims = (
    with_latest
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.account_type",
        "e.balance",
        "e.txn_type",
        F.col("b.country"),
        F.col("b.signup_date"),
    )
)

enriched = (
    with_dims
    .filter(F.col("country").isin(["DE", "SG"]))
    .withColumn("running_balance", F.sum("balance").over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
    .withColumn("balance_bucket", F.ntile(4).over(w4))
    .withColumn("prev_balance", F.lag("balance", 2, 0).over(w5))
)

with_latest_accounts = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"), F.col("c.txn_time"))
)

result = (
    with_latest_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "country",
        "signup_date",
        "running_balance",
        "cumulative_balance",
        "balance_bucket",
        "transaction_amount",
        "txn_time",
        F.sum("transaction_amount").over(w_grp).alias("last7_sum_transaction_amount"),
        F.avg("transaction_amount").over(w_grp).alias("last7_avg_transaction_amount"),
        F.max("transaction_amount").over(w_grp).alias("last7_max_transaction_amount"),
        F.last("balance_bucket", ignorenulls=True).over(w8).alias("last_known_balance_bucket"),
    )
)
