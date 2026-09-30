from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
        F.max(F.col("s.delivered_date")).alias("latest_delivered_date"),
    )
    .orderBy(F.col("latest_delivered_date").desc())
)
