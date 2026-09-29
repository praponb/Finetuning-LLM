from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isNull() & (F.col("rating") <= 2.6)) | (F.length("supplier_name") < 8))
    .select(
        "supplier_name",
        "rating",
        "city",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
    .orderBy("rating")
)
