from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("units").alias("min_units"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("avg_units") < 24)
    .select("store_id", "min_units", "avg_units")
    .orderBy(F.col("store_id").desc(), F.col("min_units").desc())
)
