from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([15, 20]) & F.col("product_id").isin([20, 27, 35])))
    .select(
        "product_id",
        "quantity",
        "order_id",
        F.ceil("quantity").alias("quantity_whole"),
    )
)
