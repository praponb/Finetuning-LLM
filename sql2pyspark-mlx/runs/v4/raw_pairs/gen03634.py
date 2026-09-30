from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("account_id").isin([3, 22, 33]) & F.col("amount").between(3700, 3900))
    .withColumn("is_credit", F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))
)

agg = (
    src
    .groupBy("is_flagged", F.hour("txn_time").alias("txn_hour"))
    .agg(F.avg("is_credit").alias("avg_is_credit"))
)

result = (
    agg
    .filter(F.col("avg_is_credit") > 24)
    .select("is_flagged", "txn_hour")
    .orderBy(F.col("txn_hour").desc())
)
