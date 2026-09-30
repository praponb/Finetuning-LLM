from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.6) & (F.col("city") == "Singapore"))
    .select(
        "id",
        "rating",
        "supplier_name",
        F.col("rating").cast("int").alias("rating_int"),
    )
)
