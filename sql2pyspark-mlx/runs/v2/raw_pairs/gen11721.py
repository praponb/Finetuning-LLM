from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 38)
    .select("id", "store_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("discount") > 0.22)
        .select("id", "store_id", "channel")
    )
    .orderBy("channel")
)
