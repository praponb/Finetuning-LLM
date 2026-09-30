from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 8, 23]))
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([20, 38]))
        .select("id", "product_id")
    )
    .orderBy("product_id")
)
