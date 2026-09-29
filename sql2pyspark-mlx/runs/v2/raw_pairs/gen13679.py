from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("o.store_id").isNull() & (F.col("o.shipping_fee") <= 13))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.order_date"),
        F.col("st.store_name"),
    )
)
