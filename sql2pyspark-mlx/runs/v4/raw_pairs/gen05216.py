from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 9)
    .select("id", "order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([4, 10, 20, 23]))
        .select("id", "order_id", "product_id")
    )
    .orderBy("order_id")
)
