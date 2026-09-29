from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.2)
    .cube(
        "city",
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max("rating").alias("max_rating"))
)
