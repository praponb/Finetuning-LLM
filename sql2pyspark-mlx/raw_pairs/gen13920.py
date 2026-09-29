from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.month(F.col("c.signup_date")).isin([2, 6, 11]))
    .groupBy(F.col("o.status"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
        F.max(F.col("s.shipped_date")).alias("latest_shipped_date"),
    )
    .orderBy("latest_shipped_date")
)
