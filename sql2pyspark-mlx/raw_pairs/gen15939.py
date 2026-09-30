from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & F.col("city").isin(["Berlin", "London", "Chiang Mai"]) & (F.upper(F.trim("supplier_name")) == "SIAM PARTS CO"))
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max("rating").alias("max_rating"))
)
