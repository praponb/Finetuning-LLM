from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.when(F.col("sqft") >= 16438, "High").when(F.col("sqft") >= 4846, "Medium").otherwise("Low").alias("sqft_level"),
    )
)

result = (
    filtered
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 13947, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("sqft_per_tokyo"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.max("sqft").alias("max_sqft"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
)
