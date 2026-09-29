from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.count(F.when(F.col("o.channel") == "web", 1)).alias("web_count"),
        (F.max(F.col("o.amount")) - F.min(F.col("o.amount"))).alias("amount_range"),
    )
    .orderBy("web_count", "is_active")
    .limit(10)
)
