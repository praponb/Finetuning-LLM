from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull() & (F.col("city") == "Berlin"))
    .groupBy(
        "city",
        F.when(F.col("country").isin(["SG", "JP"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") >= 21)
)
