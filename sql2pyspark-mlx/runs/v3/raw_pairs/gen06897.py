from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter((F.col("st.sqft") >= 8989) & F.col("o.shipping_fee").between(26, 39))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("st.region"),
        F.col("st.store_name"),
    )
)
