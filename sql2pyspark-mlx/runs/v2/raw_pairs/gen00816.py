from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([1, 33, 40]))
    .select("order_date")
    .unionByName(df_orders.filter(F.col("discount").isNull()).select("order_date"))
)
