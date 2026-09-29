from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")
w2 = Window.partitionBy("status", "account_type")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("status", "account_type").orderBy(F.col("balance").asc_nulls_last(), "id")
w5 = Window.partitionBy("account_type").orderBy("transaction_txn_time", "id")
w6 = Window.partitionBy("account_type").orderBy("transaction_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_accounts
    .select(
        "*",
        F.count("balance").over(w).alias("status_count_balance"),
        F.count("balance").over(w2).alias("status_count_balance_2"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

latest = (
    with_last
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.col("transaction_txn_time") - F.lag("transaction_txn_time").over(w5) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w6))

joined = (
    islands
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.age"), F.col("d.segment"), F.col("d.credit_limit"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.age").alias("customer_age"))
)

result = (
    with_dims
    .groupBy("customer_id")
    .pivot("account_type", ["LOAN", "SAVINGS"])
    .agg(F.sum("transaction_amount"))
)
