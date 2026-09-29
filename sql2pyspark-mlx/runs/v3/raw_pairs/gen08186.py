from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 210)
    .select("product_id", "quantity")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([12, 17, 20, 25]))
        .select("product_id", "quantity")
    )
)
