from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 2, 17, 24]))
    .select("product_id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([1, 10]))
        .select("product_id", "order_id")
    )
    .orderBy(F.col("order_id").desc())
)
