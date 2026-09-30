from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "JP", "TH"]) & F.col("rating").between(3.1, 3.5))
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") >= 1.9)
    .drop("avg_rating")
    .orderBy(F.col("tokyo_count").desc(), "city")
    .limit(20)
)
