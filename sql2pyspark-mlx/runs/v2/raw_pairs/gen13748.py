from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([25, 32, 39]))
    .select("id", "order_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isin([25, 38, 40]))
        .select("id", "order_id", "quantity")
    )
    .distinct()
)
