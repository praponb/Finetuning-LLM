from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & F.col("city").isNull())
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("supplier_count"),
        F.sum("rating").alias("total_rating"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating")).otherwise(0)).alias("th_rating"),
    )
)
