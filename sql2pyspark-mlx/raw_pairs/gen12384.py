from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.col("st.region"))
    .agg(
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
        F.min(F.col("s.cost")).alias("min_cost"),
    )
)
