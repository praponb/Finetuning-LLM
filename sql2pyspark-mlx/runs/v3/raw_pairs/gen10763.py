from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("status")
)
