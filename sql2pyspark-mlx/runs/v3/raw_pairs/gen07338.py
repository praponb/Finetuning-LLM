from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select(
        "supplier_name",
        "rating",
        "city",
        F.regexp_replace("supplier_name", " ", "-").alias("supplier_name_slug"),
    )
)
