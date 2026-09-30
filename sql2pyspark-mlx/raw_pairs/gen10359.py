from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.cost"),
        F.col("o.channel"),
        F.col("st.sqft"),
        F.col("st.city"),
        F.when(F.col("s.cost") >= 102, "High").when(F.col("s.cost") >= 17, "Medium").otherwise("Low").alias("cost_level"),
    )
    .orderBy("channel")
)
