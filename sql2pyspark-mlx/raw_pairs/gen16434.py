from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "London") & (F.col("rating") < 1.8))
    .select(
        "supplier_name",
        "rating",
        "city",
        F.concat("city", F.lit(", "), "supplier_name").alias("city_supplier_name"),
    )
)
