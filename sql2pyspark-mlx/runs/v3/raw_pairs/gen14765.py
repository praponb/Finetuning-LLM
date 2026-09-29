from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .select("order_id", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("quantity").between(4, 6))
        .select("order_id", "product_id")
    )
)
