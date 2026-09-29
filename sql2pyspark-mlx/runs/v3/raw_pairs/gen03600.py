from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(F.col("st.city"))
    .agg(
        F.max(F.col("s.cost")).alias("max_cost"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .orderBy("city")
)
