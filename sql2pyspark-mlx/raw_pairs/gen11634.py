from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("st.region") == "West")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("st.opened_date"),
        F.col("st.sqft"),
    )
    .orderBy("opened_date")
)
