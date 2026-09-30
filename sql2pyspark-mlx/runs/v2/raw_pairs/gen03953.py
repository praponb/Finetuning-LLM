from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("s.carrier"),
        F.col("o.order_date"),
        F.col("st.city"),
        F.col("st.store_name"),
    )
    .orderBy("order_date", "city")
)
