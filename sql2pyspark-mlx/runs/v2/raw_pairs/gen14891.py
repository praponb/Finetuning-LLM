from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
    )
)
