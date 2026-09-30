from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("status").alias("label"))
    .intersect(df_transactions.select(F.col("txn_type").alias("label")))
    .orderBy("label")
)
