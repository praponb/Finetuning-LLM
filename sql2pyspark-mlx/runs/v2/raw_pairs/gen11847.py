from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "London")
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.max(F.col("o.order_date")).alias("last_order_date"),
        F.sum(F.when(F.col("o.status") == "CANCELLED", F.col("o.discount")).otherwise(0)).alias("cancelled_discount"),
    )
    .orderBy(
        F.col("last_order_date").desc(),
        F.col("cancelled_discount").desc(),
        "opened_month",
    )
    .limit(50)
)
