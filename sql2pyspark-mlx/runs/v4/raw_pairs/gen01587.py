from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "London") & F.upper("supplier_name").like("%LOB%"))
    .select(
        "id",
        "city",
        "supplier_name",
        F.concat("city", F.lit(", "), "supplier_name").alias("city_supplier_name"),
    )
)
