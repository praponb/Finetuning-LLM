from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"), F.col("c.amount"))
)

result = (
    with_latest
    .withColumn("amount_tier", F.when(F.col("amount") >= 4000, "LARGE").otherwise("SMALL"))
    .groupingSets([["status", "amount_tier"], ["status"]], "status", "amount_tier")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.max("amount").alias("max_amount"),
        F.count_if(F.col("account_type") == "LOAN").alias("loan_events"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy(F.col("avg_balance").asc_nulls_first())
)
