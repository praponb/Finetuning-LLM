from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2025-01-05")
    .select("order_date", "channel")
    .subtract(
        df_orders
        .filter(F.col("store_id").isin([4, 18, 21]))
        .select("order_date", "channel")
    )
    .orderBy("channel")
)
