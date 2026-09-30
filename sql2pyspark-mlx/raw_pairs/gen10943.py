from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 16)
    .select("store_id")
    .unionByName(
        df_orders
        .filter(F.col("customer_id").isin([1, 19, 27]))
        .select("store_id")
    )
)
