from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["Tokyo", "Singapore"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("avg_rating") >= 50)
    .select("city_group", "avg_rating", "rating_count")
    .orderBy(F.col("avg_rating").desc())
)
