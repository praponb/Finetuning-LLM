from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 20)
    .select("id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isin([4, 10, 33, 38]))
        .select("id", "order_id")
    )
)
