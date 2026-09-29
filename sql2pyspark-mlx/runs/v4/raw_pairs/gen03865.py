from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNull())
    .select("id", "store_id", "order_date")
    .subtract(
        df_orders
        .filter(F.col("customer_id") == 4)
        .select("id", "store_id", "order_date")
    )
)
