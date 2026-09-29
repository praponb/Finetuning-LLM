from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        "country",
        F.concat("supplier_name", F.lit("/"), "city").alias("supplier_name_city"),
    )
)
