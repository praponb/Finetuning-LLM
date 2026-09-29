from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([9, 13, 27]))
    .select("store_id")
    .unionByName(
        df_orders
        .filter(F.col("order_date") >= "2024-05-01")
        .select("store_id")
    )
)
