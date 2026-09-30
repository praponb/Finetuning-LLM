from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .filter(~(F.col("s.order_id").isNotNull() & (F.col("s.cost") >= 85)))
    .select(F.col("s.id"), F.col("s.cost"), F.col("o.customer_id"), F.col("st.city"))
)
