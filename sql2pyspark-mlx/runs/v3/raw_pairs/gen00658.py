from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("channel").isin(["store", "web", "mobile"]))
    .groupBy("store_id")
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.avg_shipping_fee") > 38)
    .select(F.col("s.id"), F.col("s.region"), F.col("st.avg_shipping_fee"))
)
