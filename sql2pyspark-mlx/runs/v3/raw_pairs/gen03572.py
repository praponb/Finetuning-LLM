from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2400)
    .groupBy("store_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .filter(F.col("avg_units") > 28)
    .select("store_id", "avg_units")
    .orderBy(F.col("store_id").desc(), "avg_units")
)
