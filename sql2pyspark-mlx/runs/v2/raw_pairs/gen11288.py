from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .filter((F.col("st.sqft") > 4807) & F.col("st.region").isin(["South", "West", "East"]))
    .select(
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.status"),
        F.col("st.city"),
    )
)
