from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eu%"))
    .select(
        "id",
        "supplier_name",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.lower("supplier_name").alias("supplier_name_lower"),
    )
)
