from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("delivered_date") <= "2023-04-10").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("o.amount"),
    )
)
