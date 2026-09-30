from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "UK"]))
    .select(
        "id",
        "country",
        F.concat_ws(",", "city", "supplier_name").alias("city_supplier_name_key"),
    )
)
