from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([1, 2, 24]))
    .select("id", "channel", "order_date")
    .subtract(
        df_orders
        .filter(F.col("amount") >= 1130)
        .select("id", "channel", "order_date")
    )
    .orderBy("order_date")
)
