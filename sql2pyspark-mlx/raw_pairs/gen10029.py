from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "US", "TH"]) | (F.upper(F.trim("supplier_name")) == "SIAM PARTS CO"))
)

suppliers_stats = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max("rating").alias("max_rating"))
)

result = (
    suppliers_stats
    .select("rating_bucket", "max_rating")
    .orderBy("max_rating", "rating_bucket")
    .limit(5)
)
