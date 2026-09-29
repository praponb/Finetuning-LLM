from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select(
        "id",
        "rating",
        "country",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)
