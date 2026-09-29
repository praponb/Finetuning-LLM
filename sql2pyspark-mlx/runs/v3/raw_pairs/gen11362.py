from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([9, 27, 28, 39]))
    .select("id", "product_id", "quantity")
    .intersect(
        df_order_items
        .filter(F.col("unit_price").between(150, 300))
        .select("id", "product_id", "quantity")
    )
)
