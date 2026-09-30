from pyspark.sql import functions as F

result = (
    df_orders
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["city", "channel"], ["city"], []],
        F.col("c.city").alias("city"),
        F.col("a.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.discount")).alias("avg_discount"),
        F.max(F.col("a.amount")).alias("max_amount"),
    )
)
