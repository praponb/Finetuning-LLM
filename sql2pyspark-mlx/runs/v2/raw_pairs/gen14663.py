from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .select(
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("st.opened_date"),
    )
)
