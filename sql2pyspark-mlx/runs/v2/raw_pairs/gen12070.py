from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(
        F.col("s.region"),
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("o.status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
        F.sum(F.col("o.shipping_fee")).alias("total_shipping_fee"),
    )
    .filter(F.col("shipped_count") > 66)
)
