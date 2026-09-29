from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("s.status") == "LOST")
    .groupBy(F.col("o.status"))
    .agg(
        F.max(F.col("s.shipped_date")).alias("latest_shipped_date"),
        F.sum(F.when(F.col("s.carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
    )
    .orderBy(F.col("dhl_count").desc(), "status")
    .limit(5)
)
