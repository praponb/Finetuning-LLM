from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("o.amount"),
        F.col("st.sqft"),
    )
)
