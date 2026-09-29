from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.col("order_date").between("2023-04-01", "2025-04-01"))
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("order_date").alias("latest_order_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.age"),
        F.col("ct.status_values"),
        F.col("ct.latest_order_date"),
    )
    .orderBy("city")
)
