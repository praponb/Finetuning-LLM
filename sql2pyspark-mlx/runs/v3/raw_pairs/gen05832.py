from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .groupBy(F.col("st.region"))
    .agg(F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"))
    .orderBy("region")
    .limit(50)
)
