from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 9, 14, 27]))
    .select("id", "quantity", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("product_id") == 39)
        .select("id", "quantity", "product_id")
    )
)
