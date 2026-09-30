from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 30) & F.col("product_id").isin([36, 37, 38]))
    .select(
        "quantity",
        "product_id",
        "order_id",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)
