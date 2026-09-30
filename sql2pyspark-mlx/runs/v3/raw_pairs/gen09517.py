from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.city"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sum(F.col("o.amount")).alias("total_amount"),
        F.count(F.when(F.col("o.status") == "PENDING", 1)).alias("pending_count"),
    )
)
