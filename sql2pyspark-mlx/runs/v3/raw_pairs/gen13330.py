from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "country",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)
