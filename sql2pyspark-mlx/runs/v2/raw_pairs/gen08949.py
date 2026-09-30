from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 10, 12, 14]))
    .select("product_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([5, 16, 20, 33]))
        .select("product_id")
    )
    .orderBy(F.col("product_id").desc())
)
