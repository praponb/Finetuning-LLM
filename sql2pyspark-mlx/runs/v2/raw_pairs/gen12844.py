from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .groupBy(F.col("s.city"), F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0)).alias("web_count"),
        F.count("*").alias("row_count"),
    )
)
