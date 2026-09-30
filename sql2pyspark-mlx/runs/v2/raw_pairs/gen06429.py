from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter((F.col("o.channel") == "web") & (F.col("s.delivered_date") <= "2025-07-26"))
    .select(
        F.col("s.order_id"),
        F.col("s.carrier"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("st.region"),
        F.col("st.opened_date"),
    )
    .orderBy(F.col("order_id").desc())
)
