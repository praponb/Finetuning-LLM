from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & (F.col("city").isin(["Berlin", "Tokyo"]) | (F.col("country") == "JP")))
)

agg = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("rating").alias("rating_count"),
    )
)

result = agg.select("rating_bucket", "country", "total_rating", "rating_count")
