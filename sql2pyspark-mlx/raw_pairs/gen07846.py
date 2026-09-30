from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        F.concat("country", F.lit("/"), "supplier_name").alias("country_supplier_name"),
    )
)
