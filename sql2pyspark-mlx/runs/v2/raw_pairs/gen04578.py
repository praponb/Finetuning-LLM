from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 3)
    .select("id", "order_id", "quantity")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") >= 330)
        .select("id", "order_id", "quantity")
    )
)
