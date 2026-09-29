from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.lower("supplier_name").alias("supplier_name_lower"),
    )
)
