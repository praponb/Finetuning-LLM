from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.order_date"),
        F.col("st.sqft"),
        F.col("st.store_name"),
    )
)
