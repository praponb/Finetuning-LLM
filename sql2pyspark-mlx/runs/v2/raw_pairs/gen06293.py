from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.month(F.col("st.opened_date")).isin([2, 5, 10]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.customer_id"),
        F.col("st.region"),
        F.col("st.sqft"),
    )
    .orderBy("id")
    .limit(10)
)
