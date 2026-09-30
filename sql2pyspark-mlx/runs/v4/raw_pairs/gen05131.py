from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([11, 15, 25, 30]))
    .select("id", "order_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("quantity") != 6)
        .select("id", "order_id", "quantity")
    )
    .distinct()
    .orderBy("order_id")
)
