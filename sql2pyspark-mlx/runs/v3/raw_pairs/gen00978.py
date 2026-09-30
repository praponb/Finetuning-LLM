from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("id", "product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([5, 7, 31]))
        .select("id", "product_id", "order_id")
    )
)
