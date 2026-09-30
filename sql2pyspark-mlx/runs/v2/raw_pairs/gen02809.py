from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("o.channel") == "partner")
    .groupBy(F.col("o.status"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
        F.min(F.col("s.delivered_date")).alias("min_delivered_date"),
    )
    .orderBy("min_delivered_date", "status")
    .limit(10)
)
