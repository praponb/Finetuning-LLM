from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        "city",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
    .filter(F.col("rating_range") < 44)
)
