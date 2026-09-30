from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        F.coalesce("supplier_name", F.lit("unassigned")).alias("supplier_name_filled"),
    )
    .orderBy("supplier_name_filled")
)
