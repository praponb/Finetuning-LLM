from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["East", "North", "South"])
    .agg(F.avg("sqft").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
