from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") < 14) & (F.col("city").isin(["London", "Tokyo"]) | F.col("city").isin(["Bangkok", "New York", "Chiang Mai"])))
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy(F.col("rating_count").desc(), F.col("min_rating").desc(), "rating_bucket")
    .limit(20)
)
