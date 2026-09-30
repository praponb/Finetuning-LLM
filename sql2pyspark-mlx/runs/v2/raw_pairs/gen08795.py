from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([10, 28, 34]))
    .select("product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([24, 25]))
        .select("product_id")
    )
    .orderBy("product_id")
)
