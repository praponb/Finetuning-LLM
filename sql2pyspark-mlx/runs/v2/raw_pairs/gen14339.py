from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 14)
    .select("order_date", "channel")
    .unionByName(
        df_orders
        .filter(F.col("customer_id").isin([10, 15, 25, 28]))
        .select("order_date", "channel")
    )
    .distinct()
)
