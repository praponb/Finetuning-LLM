from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
        F.sum(F.when(F.col("s.status") == "LOST", F.col("s.cost")).otherwise(0)).alias("lost_cost"),
    )
    .filter(F.col("lost_cost") > 28)
    .orderBy(F.col("lost_cost").desc())
)
