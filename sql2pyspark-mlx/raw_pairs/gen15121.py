from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "right")
    .filter(F.month(F.col("s.shipped_date")).isin([3, 8, 9]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("st.region"),
        F.col("st.store_name"),
    )
    .orderBy(F.col("id").desc())
)
