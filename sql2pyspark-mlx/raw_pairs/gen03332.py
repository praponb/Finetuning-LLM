from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter((F.col("rating") <= 3.8) & ~F.col("country").isin(["TH", "JP"]))

city_summary = (
    valid_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
)

result = (
    city_summary
    .filter(F.col("total_rating") >= 11)
    .select("city", "rating_bucket", "total_rating")
)
