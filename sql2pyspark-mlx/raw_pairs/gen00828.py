from pyspark.sql import functions as F

result = (
    df_suppliers
    .rollup(
        "country",
        F.when(F.col("rating") >= 3.8, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("rating_per_jp"),
    )
    .orderBy("us_rating")
)
