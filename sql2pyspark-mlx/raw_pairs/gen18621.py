from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "right")
    .filter(F.col("s.order_id").isin([6, 10, 13]))
    .select(
        F.col("s.delivered_date"),
        F.col("o.discount"),
        F.col("st.city"),
        F.col("st.sqft"),
        F.length(F.col("st.store_name")).alias("store_name_length"),
    )
)
