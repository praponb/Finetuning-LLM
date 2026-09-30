from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG"]))
    .withColumn("rating_band", F.when(F.col("rating") >= 4.1, "High").when(F.col("rating") >= 2.1, "Medium").otherwise("Low"))
)

city_summary = (
    src
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.round(((100 * F.sum(F.when(F.col("country") == "SG", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sg"),
    )
)

result = (
    city_summary
    .filter(F.col("pct_sg") >= 65)
    .select("city", "tokyo_count")
    .orderBy("tokyo_count", "city")
)
