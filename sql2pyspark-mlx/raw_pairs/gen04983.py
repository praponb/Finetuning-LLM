from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["TH", "DE"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .orderBy(F.col("rating_range").desc())
)
