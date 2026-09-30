from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([28, 37]))
    .select("id", "product_id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isin([3, 33, 36]))
        .select("id", "product_id", "order_id")
    )
)
