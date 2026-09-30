from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .select("id", "quantity", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id") == 5)
        .select("id", "quantity", "product_id")
    )
)
