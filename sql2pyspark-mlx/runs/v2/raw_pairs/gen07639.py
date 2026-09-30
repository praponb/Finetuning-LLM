from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "tags",
        "cost",
        (F.col("price") - F.col("cost")).alias("margin"),
    )
)
