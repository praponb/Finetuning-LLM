from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 3.7)
    .groupBy(
        F.when(F.col("country").isin(["JP", "TH"]), "priority").otherwise("other").alias("country_group"),
        "city",
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("rating_count").desc(), "country_group")
)
