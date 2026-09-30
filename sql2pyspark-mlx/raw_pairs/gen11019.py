from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("supplier_name", F.lit(", "), "city").alias("supplier_name_city"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

result = (
    valid
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "DE")
        .select("id")
    )
)
