from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-11").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "region", "city").alias("region_city_key"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .groupBy("region")
    .pivot("city", ["Singapore", "Chiang Mai"])
    .agg(F.sum("sqft"))
    .select(
        "region",
        F.col("Singapore").alias("singapore"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
