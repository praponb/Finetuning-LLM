from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "supplier_name",
        "city",
        F.upper(F.trim("supplier_name")).alias("supplier_name_normalized"),
    )
)
