from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2023-03-01")
    .select("customer_id", "channel")
    .intersect(
        df_orders
        .filter(F.col("shipping_fee") > 34)
        .select("customer_id", "channel")
    )
)
