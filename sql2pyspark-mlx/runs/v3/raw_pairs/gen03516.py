from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("s.delivered_date") > "2025-01-01")
    .groupBy(F.col("o.channel"))
    .agg(F.min(F.col("s.delivered_date")).alias("first_delivered_date"))
    .orderBy("channel")
    .limit(50)
)
