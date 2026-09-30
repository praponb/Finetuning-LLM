from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("o.status").isin(["RETURNED", "SHIPPED"]))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.status"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("st.opened_date"),
        F.datediff(F.current_date(), F.col("s.shipped_date")).alias("days_since_shipped_date"),
    )
)
