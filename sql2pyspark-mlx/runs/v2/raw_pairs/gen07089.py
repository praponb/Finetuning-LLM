from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.5, 4.1) | F.col("city").isNotNull())
    .select(
        "id",
        "rating",
        "country",
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
    )
)
