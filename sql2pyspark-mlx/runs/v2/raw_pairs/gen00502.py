from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Bangkok"]) & (F.col("city") == "Chiang Mai"))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("num_suppliers"),
    )
    .orderBy(F.col("avg_rating").desc(), "city", "rating_bucket")
    .limit(1)
)
