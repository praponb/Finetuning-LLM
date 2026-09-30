from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.sum(F.when(F.col("s.status") == "RETURNED", F.col("s.cost")).otherwise(0)).alias("returned_cost"),
    )
    .orderBy("channel", "returned_cost")
)
