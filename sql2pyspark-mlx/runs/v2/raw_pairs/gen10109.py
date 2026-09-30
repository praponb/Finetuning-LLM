from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "COMPLETED"]))
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("cs.channel_values"),
        F.col("cs.status_values"),
    )
    .orderBy("credit_limit", "status_values")
)
