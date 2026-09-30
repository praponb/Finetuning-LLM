from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") >= F.lit("2023-02-19").cast("date")))
    .select(
        "*",
        F.concat_ws("/", "store_name", "region").alias("store_name_region_key"),
    )
)

result = (
    base
    .groupBy("region")
    .pivot("city", ["Berlin", "Bangkok"])
    .agg(F.avg("sqft"))
)
