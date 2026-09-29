from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 11)
    .select("id", "store_id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("customer_id") == 8)
        .select("id", "store_id", "order_date")
    )
)
