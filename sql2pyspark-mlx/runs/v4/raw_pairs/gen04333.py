from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("st.opened_date") > "2025-03-10")
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("o.status"),
        F.col("st.opened_date"),
        F.col("st.sqft"),
    )
    .orderBy("opened_date", "id")
    .limit(5)
)
