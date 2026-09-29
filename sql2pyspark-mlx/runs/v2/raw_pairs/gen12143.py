from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "rating",
        "city",
        "supplier_name",
        F.col("rating").cast("int").alias("rating_int"),
        F.substring("supplier_name", 1, 3).alias("supplier_name_prefix"),
    )
)
