from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2023-04-01")
    .select("channel")
    .unionByName(df_orders.filter(F.col("customer_id").isNull()).select("channel"))
    .distinct()
)
