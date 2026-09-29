from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%") & (F.col("supplier_name").like("T%") | (F.col("country") == "UK")))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") >= 2)
    .orderBy("country")
)
