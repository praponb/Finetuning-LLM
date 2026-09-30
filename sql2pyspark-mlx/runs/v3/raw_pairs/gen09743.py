from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.status") == "RETURNED", F.col("s.cost")).otherwise(0)).alias("returned_cost"),
    )
    .orderBy("returned_cost")
)
