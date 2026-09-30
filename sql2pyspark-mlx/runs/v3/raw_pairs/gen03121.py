from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") | (F.col("rating") < 4.0))
    .select(
        "rating",
        "country",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.trim("supplier_name").alias("clean_supplier_name_2"),
    )
)
