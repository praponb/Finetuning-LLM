from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

result = (
    with_last
    .groupBy("account_type")
    .agg(
        F.stddev_samp("balance").alias("balance_stddev"),
        F.sort_array(F.collect_set("transaction_txn_type")).alias("transaction_txn_type_list"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("avg_balance") > 0)
    .orderBy(F.col("account_type").desc())
)
