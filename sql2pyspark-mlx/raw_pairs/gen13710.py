from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.discount"),
        F.col("o.channel"),
        F.col("st.sqft"),
        F.col("st.opened_date"),
    )
    .orderBy("status", "id")
    .limit(3)
)
