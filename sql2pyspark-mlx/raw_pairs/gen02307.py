from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-03-16").cast("date")) & F.col("store_name").isNull())
    .withColumn("opened_date_plus_77d", F.date_add("opened_date", 77))
    .withColumn("resolved_region", F.coalesce("region", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 17781, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("region").desc())
)
