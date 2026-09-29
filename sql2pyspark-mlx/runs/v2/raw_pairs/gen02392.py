from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id") == 3)
    .select("id", "quantity", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id") == 10)
        .select("id", "quantity", "order_id")
    )
)
