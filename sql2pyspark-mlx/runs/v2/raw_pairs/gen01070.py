from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("country") == "TH") & (F.col("city") == "New York")) | F.col("rating").between(2.0, 3.8))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.min("rating").alias("min_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy(F.col("avg_rating").desc(), "country")
)
