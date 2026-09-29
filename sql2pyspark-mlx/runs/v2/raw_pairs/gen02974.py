from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([29, 32, 38]))
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id") == 7)
        .select("id", "product_id")
    )
    .orderBy("product_id")
)
