from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 14, 39]))
    .select("id", "order_id", "quantity")
    .intersect(
        df_order_items
        .filter(F.col("quantity") != 7)
        .select("id", "order_id", "quantity")
    )
)
