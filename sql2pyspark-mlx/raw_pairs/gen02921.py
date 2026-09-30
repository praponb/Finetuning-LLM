from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "SG", "US"]))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.max("rating").alias("max_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
    )
    .filter(F.col("rating_range") > 59)
)
