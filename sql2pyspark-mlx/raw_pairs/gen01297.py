from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & (F.col("city") == "Berlin"))
)

suppliers_stats = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("avg_rating") > 57)
    .orderBy(F.col("avg_rating").desc(), "rating_bucket")
)
