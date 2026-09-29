from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("shipped_date") <= "2023-12-01")
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("shipped_date").alias("min_shipped_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("t.status_values"),
        F.col("t.min_shipped_date"),
    )
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
    .limit(50)
)
