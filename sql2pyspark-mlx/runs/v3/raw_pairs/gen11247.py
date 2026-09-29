from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["JP", "DE"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .filter(F.col("rating_range") < 70)
    .select("country_group", "avg_rating", "rating_range")
    .orderBy("country_group")
)
