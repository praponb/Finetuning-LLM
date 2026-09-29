from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .select(F.col("s.delivered_date"), F.col("o.status"), F.col("st.city"))
)
