from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("st.region").isin(["North", "West", "South"]))
    .select(
        F.col("s.delivered_date"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("st.region"),
        F.col("st.sqft"),
        F.last_day(F.col("o.order_date")).alias("order_date_month_end"),
    )
    .orderBy("store_id")
)
