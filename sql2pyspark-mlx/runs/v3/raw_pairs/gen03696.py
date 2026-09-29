from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("order_id").isin([6, 12, 40]) & F.col("quantity").between(4, 6) & (F.col("quantity") > 4)))
    .select(
        "id",
        "quantity",
        "unit_price",
        "product_id",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)
