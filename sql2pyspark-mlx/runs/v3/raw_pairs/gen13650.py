from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter(F.col("st.region") == "North")
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("st.sqft"),
        F.col("st.opened_date"),
        (F.col("o.shipping_fee") - F.col("o.amount")).alias("amount_with_shipping"),
    )
)
