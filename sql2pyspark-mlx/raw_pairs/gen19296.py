from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "SG", "TH"]))
    .groupBy(
        F.when(F.col("country").isin(["UK", "DE"]), "priority").otherwise("other").alias("country_group"),
        "country",
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") > 65)
    .orderBy(F.col("stddev_rating").desc())
)
