from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([18, 36]) | F.col("quantity").isNull())
    .select(
        "id",
        "quantity",
        "product_id",
        "order_id",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)
