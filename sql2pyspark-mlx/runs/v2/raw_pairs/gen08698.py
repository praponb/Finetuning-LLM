from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "channel")
    .agg(
        F.sum(F.when(F.col("channel") == "store", F.col("amount")).otherwise(0)).alias("store_amount"),
    )
    .filter(F.col("store_amount") > 60)
    .orderBy("status", "channel")
)
