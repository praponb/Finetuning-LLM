from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("city").isin(["Berlin", "Tokyo", "London"]))

summary = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        F.count("*").alias("num_suppliers"),
    )
)

result = (
    summary
    .filter(F.col("num_suppliers") < 6)
    .orderBy(F.col("chiang_mai_count").desc(), "num_suppliers", "rating_bucket")
    .limit(10)
)
