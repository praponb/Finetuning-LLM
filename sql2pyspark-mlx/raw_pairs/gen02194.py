from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "right")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.carrier"),
        F.col("o.shipping_fee"),
        F.col("st.city"),
        F.initcap(F.trim(F.col("o.channel"))).alias("channel_title"),
    )
)
