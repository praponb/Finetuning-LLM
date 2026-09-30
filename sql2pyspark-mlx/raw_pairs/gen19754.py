from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.concat("city", F.lit(" - "), "country").alias("city_country"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

result = (
    valid
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(F.col("rating") > 2.2)
        .select("id", "city")
    )
)
