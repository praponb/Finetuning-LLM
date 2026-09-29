from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("s.delivered_date") >= "2024-12-07")
    .select(
        F.col("s.order_id"),
        F.col("s.status"),
        F.col("o.store_id"),
        F.col("st.sqft"),
        F.concat_ws("-", F.col("st.region"), F.col("st.city")).alias("region_city_key"),
    )
)
