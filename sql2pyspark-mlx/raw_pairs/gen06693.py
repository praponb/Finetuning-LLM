from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 6, 23, 29]))
    .select("product_id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([17, 24, 26, 37]))
        .select("product_id", "order_id")
    )
    .orderBy(F.col("product_id").desc())
)
