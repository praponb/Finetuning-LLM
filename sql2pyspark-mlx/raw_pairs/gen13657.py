from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .groupBy(F.col("st.region"))
    .agg(F.sum(F.col("s.cost")).alias("total_cost"))
)
