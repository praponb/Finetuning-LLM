from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(13, 21))
    .select("status", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("order_date") <= "2024-07-25")
        .select("status", "customer_id")
    )
)
