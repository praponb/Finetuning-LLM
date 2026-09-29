from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.avg("price").alias("avg_price"))
    .filter(F.col("avg_price") < 790)
    .select("launch_quarter", "avg_price")
    .orderBy("avg_price", "launch_quarter")
    .limit(5)
)
