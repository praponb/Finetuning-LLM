from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("st.city"),
        F.col("st.region"),
        F.coalesce(F.col("s.carrier"), F.lit("unassigned")).alias("carrier_filled"),
    )
)
