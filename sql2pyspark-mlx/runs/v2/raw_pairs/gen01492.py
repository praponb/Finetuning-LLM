from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount").between(840, 1270))
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.max(F.col("o.order_date")).alias("last_order_date"),
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
)
