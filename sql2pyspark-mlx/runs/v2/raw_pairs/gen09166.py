from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "DE"]))
    .select(
        "rating",
        "supplier_name",
        "city",
        F.concat_ws(",", "city", "supplier_name").alias("city_supplier_name_key"),
    )
)
