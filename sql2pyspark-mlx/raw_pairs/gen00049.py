from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("st.opened_date")).alias("opened_month"))
    .agg(F.avg(F.col("s.cost")).alias("avg_cost"))
    .orderBy("avg_cost")
)
