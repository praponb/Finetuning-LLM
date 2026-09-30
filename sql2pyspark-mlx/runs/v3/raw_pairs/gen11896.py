from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter(F.col("st.opened_date").between("2023-08-01", "2025-02-01"))
    .select(
        F.col("s.status"),
        F.col("o.status").alias("order_status"),
        F.col("o.store_id"),
        F.col("st.region"),
        F.col("st.city"),
    )
)
