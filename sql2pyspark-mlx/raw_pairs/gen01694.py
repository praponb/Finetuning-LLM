from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("s.shipped_date").between("2025-03-01", "2025-04-01"))
    .select(
        F.col("s.status"),
        F.col("s.shipped_date"),
        F.col("o.customer_id"),
        F.col("st.city"),
        F.trim(F.col("o.status")).alias("clean_status"),
    )
)
