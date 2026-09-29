from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "right")
    .filter(F.col("s.cost") < 90)
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
        F.col("st.opened_date"),
        F.col("st.city"),
    )
    .orderBy("opened_date")
)
