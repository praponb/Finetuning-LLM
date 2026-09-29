from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Chiang Mai"]))
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy("city")
)
