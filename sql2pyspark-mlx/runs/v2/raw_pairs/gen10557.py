from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("o.channel").isNotNull())
    .select(
        F.col("s.order_id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("st.sqft"),
        F.col("st.opened_date"),
    )
)
