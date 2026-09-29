from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(
        F.when(F.col("o.channel").isin(["mobile", "web"]), "group_a").otherwise("other").alias("channel_group"),
        F.year(F.col("o.order_date")).alias("order_year"),
    )
    .agg(
        F.count(F.when(F.col("s.carrier") == "DHL", 1)).alias("dhl_count"),
        F.avg(F.col("s.cost")).alias("avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc())
)
