from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .select(
        F.col("s.status"),
        F.col("o.store_id"),
        F.col("st.sqft"),
        F.col("st.opened_date"),
        F.date_trunc("MONTH", F.col("st.opened_date")).alias("opened_date_period"),
    )
    .orderBy(F.col("status").desc(), "opened_date")
)
