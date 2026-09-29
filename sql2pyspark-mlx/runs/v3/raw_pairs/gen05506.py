from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 320)
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([5, 10, 20, 29]))
        .select("id", "product_id")
    )
)
