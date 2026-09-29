from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Chiang Mai"]) & F.upper("supplier_name").like("%URO%"))
    .withColumn("rating_level", F.when(F.col("rating") >= 4.0, "High").when(F.col("rating") >= 1.5, "Medium").otherwise("Low"))
)

agg = (
    recent_suppliers
    .groupBy("country", "city")
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
    )
)

result = (
    agg
    .filter(F.col("london_rating") >= 57)
    .select("country", "city", "london_rating", "us_count")
)
