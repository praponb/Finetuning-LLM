from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", "-"))
)

rating_bucket_summary = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.sum("rating").alias("total_rating"))
)

result = (
    rating_bucket_summary
    .filter(F.col("total_rating") < 23)
    .select("rating_bucket", "total_rating")
    .orderBy(F.col("total_rating").desc(), "rating_bucket")
)
