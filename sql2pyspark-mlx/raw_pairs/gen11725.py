from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") > 1)
    .select(
        "id",
        "price",
        "supplier_id",
        F.array_contains("tags", "premium").alias("is_premium"),
    )
)
