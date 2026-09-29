from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter((F.col("o.shipping_fee") > 21) | (F.col("o.customer_id") == 1))
    .select(
        F.col("s.status"),
        F.col("o.customer_id"),
        F.col("st.region"),
        F.col("st.opened_date"),
        F.regexp_replace(F.col("st.store_name"), " ", "-").alias("store_name_slug"),
    )
)
