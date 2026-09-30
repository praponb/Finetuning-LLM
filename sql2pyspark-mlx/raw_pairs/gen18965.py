from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Berlin")))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.substring("city", 1, 4).alias("city_prefix"),
    )
)

result = (
    valid
    .select("id", "country")
    .intersect(
        df_suppliers
        .filter(~F.col("country").isin(["UK", "SG"]))
        .select("id", "country")
    )
)
