from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(F.col("st.city"))
    .agg(
        F.max(F.col("s.delivered_date")).alias("last_delivered_date"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc(), "city")
    .limit(20)
)
