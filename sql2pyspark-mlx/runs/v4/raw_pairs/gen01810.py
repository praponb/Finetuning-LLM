from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.order_id"),
        F.col("o.status"),
        F.col("st.region"),
        F.col("st.city"),
        F.coalesce(F.col("s.cost"), F.lit(0)).alias("cost_filled"),
    )
)
