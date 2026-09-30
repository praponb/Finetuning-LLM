from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 8)
    .select("id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") < 180)
        .select("id", "order_id")
    )
)
