from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 3.5) | F.col("supplier_name").like("%Lio%"))
    .select(
        "city",
        F.concat("country", F.lit("/"), "supplier_name").alias("country_supplier_name"),
    )
)
