from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 7)
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([6, 8, 14]))
        .select("id", "product_id")
    )
)
