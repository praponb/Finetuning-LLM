from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("st.region"), F.col("st.city"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
    )
)
