from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Globa%") & F.col("country").isin(["SG", "US"]) & F.lower("supplier_name").like("%aci%"))
)

rating_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.count("rating").alias("rating_count"))
)

result = rating_bucket_summary.filter(F.col("rating_count") >= 9)
