from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "country",
        "city",
        "rating",
        F.round("rating", 0).alias("rating_rounded"),
        F.concat("city", F.lit(" "), "country").alias("city_country"),
    )
    .orderBy(F.col("city").desc())
)
