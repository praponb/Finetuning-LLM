from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("o.amount") >= 1110)
    .groupBy(F.col("st.region"))
    .agg(F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"))
    .filter(F.col("avg_cost") >= 64)
    .orderBy("region", F.col("avg_cost").desc())
)
