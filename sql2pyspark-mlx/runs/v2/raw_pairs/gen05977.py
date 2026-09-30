from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & F.col("rating").between(2.9, 3.8) & (F.col("country") == "UK"))
    .withColumn("country_lower", F.lower("country"))
)

suppliers_stats = (
    filtered_suppliers
    .groupBy("city")
    .agg(F.avg("rating").alias("avg_rating"))
)

result = (
    suppliers_stats
    .filter(F.col("avg_rating") < 2.9)
    .select("city", "avg_rating")
    .orderBy(F.col("city").desc())
)
