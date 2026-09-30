from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & F.col("supplier_name").like("%Globa%") & (F.length("supplier_name") < 12))
    .select(
        "id",
        "rating",
        "supplier_name",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.floor("rating").alias("rating_whole"),
    )
)
