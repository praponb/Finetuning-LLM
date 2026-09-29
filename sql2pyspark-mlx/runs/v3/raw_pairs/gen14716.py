from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 3.6) & ((F.col("city") == "Singapore") | F.col("city").isin(["Bangkok", "Singapore", "New York"])))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
    .orderBy("min_rating")
)
