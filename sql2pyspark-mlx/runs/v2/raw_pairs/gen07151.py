from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([15, 27, 32, 36]))
    .select("product_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("order_id") == 16)
        .select("product_id", "quantity")
    )
)
