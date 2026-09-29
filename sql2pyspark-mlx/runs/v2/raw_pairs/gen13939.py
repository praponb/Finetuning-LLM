from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("st.city"), F.col("st.region"))
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
    )
    .orderBy(F.col("total_cost").desc(), "city", "region")
    .limit(10)
)
