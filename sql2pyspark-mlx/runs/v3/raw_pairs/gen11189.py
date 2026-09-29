from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
    )
)
