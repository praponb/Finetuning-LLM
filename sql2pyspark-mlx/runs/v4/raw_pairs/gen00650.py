from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.carrier"),
        F.col("o.shipping_fee"),
        F.col("o.status").alias("order_status"),
        F.col("st.opened_date"),
        F.col("st.sqft"),
        F.trim(F.col("o.channel")).alias("clean_channel"),
    )
    .orderBy("shipping_fee")
)
