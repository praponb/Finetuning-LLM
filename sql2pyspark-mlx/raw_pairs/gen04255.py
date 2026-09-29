from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([24, 34, 39]))
    .select("quantity", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") < 320)
        .select("quantity", "product_id")
    )
    .orderBy(F.col("quantity").desc())
)
