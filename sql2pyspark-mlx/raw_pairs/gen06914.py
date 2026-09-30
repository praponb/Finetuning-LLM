from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.cost") < 27)
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.carrier"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("c.signup_date"),
        F.last_day(F.col("o.order_date")).alias("order_date_month_end"),
    )
)
