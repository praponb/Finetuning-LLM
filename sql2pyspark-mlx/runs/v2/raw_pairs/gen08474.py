from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(
        F.col("st.city"),
        F.date_trunc("MONTH", F.col("st.opened_date")).alias("opened_month"),
    )
    .agg(
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.count(F.col("s.cost")).alias("cost_count"),
    )
    .orderBy(F.col("city").desc())
)
