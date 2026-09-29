from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("city").isin(["Bangkok", "Berlin"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .filter(F.col("avg_rating") < 26)
    .select("city", "city_group", "rating_range", "avg_rating")
)
