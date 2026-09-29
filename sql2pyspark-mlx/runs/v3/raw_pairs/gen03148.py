from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .select(
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("st.opened_date"),
        F.col("st.sqft"),
    )
    .orderBy("shipped_date")
)
