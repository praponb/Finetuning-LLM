from pyspark.sql import functions as F

result = (
    df_order_items
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") > 7)
        .select("order_id", "product_id")
    )
)
