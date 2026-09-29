from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .groupBy("channel", "status")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") < 9300)
    .orderBy(F.col("total_amount").desc(), "channel")
)
