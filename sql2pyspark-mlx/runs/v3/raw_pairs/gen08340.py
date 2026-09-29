from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.cost") <= 48)
    .select(
        F.col("s.delivered_date"),
        F.col("s.shipped_date"),
        F.col("o.channel"),
        F.last_day(F.col("s.delivered_date")).alias("delivered_date_month_end"),
    )
)
