from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "outer")
    .filter(F.col("s.status") != "RETURNED")
    .groupBy(F.col("o.customer_id"))
    .agg(F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"))
    .orderBy("customer_id")
)
