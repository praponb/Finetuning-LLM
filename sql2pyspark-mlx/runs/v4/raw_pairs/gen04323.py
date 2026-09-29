from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 120)
    .select("id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isin([4, 12, 26, 36]))
        .select("id", "order_id")
    )
    .orderBy("id")
)
