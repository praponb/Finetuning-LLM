from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.customer_id").isin([8, 13, 22]))
    .groupBy(
        F.col("orders.channel"),
        F.when(F.year(F.col("orders.order_date")) < 2024, "before_2024").when(F.year(F.col("orders.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.count(F.col("shipments.cost")).alias("cost_count"),
        F.avg(F.col("shipments.cost")).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") > 52)
)
