from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter((F.length("supplier_name") < 8) & (F.col("city") == "New York") & (F.length("supplier_name") >= 11))
)

country_summary = (
    base
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)

result = (
    country_summary
    .orderBy("rating_bucket", "country")
    .limit(3)
)
