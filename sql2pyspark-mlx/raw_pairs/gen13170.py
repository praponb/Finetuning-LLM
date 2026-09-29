from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        F.concat_ws("-", "city", "supplier_name").alias("city_supplier_name_key"),
    )
    .orderBy("id", "city_supplier_name_key")
)
