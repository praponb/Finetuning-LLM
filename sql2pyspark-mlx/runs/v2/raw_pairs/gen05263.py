from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("o.order_date"),
        F.col("st.store_name"),
        F.col("st.sqft"),
    )
    .orderBy("store_name", "cost")
)
