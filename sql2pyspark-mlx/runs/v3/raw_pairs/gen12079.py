from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 16, 27, 34]))
    .select("product_id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") != 6)
        .select("product_id", "order_id")
    )
    .orderBy(F.col("product_id").desc())
)
