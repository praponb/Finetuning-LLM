from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "country",
        "supplier_name",
        "rating",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
        F.concat_ws(",", "country", "supplier_name").alias("country_supplier_name_key"),
    )
)
