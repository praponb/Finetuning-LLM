from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 2.7)
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
)
