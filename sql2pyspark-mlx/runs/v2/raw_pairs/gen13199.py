from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([20, 21, 26, 28]))
    .select("product_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") == 6)
        .select("product_id", "quantity")
    )
    .distinct()
)
