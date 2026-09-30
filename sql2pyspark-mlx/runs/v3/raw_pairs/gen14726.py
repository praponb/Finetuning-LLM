from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .filter(F.col("avg_units") > 53)
    .select("product_id", "avg_units")
    .orderBy(F.col("product_id").desc())
)
