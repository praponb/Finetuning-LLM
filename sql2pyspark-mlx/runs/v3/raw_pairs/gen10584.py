from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([12, 21, 32]))
    .select("id", "order_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("product_id") == 5)
        .select("id", "order_id", "quantity")
    )
    .orderBy("quantity")
)
