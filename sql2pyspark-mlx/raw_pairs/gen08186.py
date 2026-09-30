from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["Corporate", "SMB", "Consumer"])
    .agg(F.avg("age").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
