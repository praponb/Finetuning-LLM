from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .select("id", "product_id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") >= 190)
        .select("id", "product_id", "order_id")
    )
    .orderBy(F.col("id").desc())
)
