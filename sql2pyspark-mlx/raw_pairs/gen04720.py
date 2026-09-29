from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "inner")
    .groupBy(F.col("st.region"))
    .agg(
        F.avg(F.col("s.cost")).alias("avg_cost"),
        F.count(F.when(F.col("s.status") == "RETURNED", 1)).alias("returned_count"),
    )
    .orderBy(F.col("returned_count").desc())
)
