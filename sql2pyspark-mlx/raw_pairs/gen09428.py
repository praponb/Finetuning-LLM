from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.order_date"),
        F.col("st.store_name"),
        F.col("st.region"),
    )
    .orderBy("order_date")
)
