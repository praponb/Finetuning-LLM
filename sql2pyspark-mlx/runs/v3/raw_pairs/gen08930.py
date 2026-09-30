from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("o.shipping_fee").between(24, 33))
    .groupBy(F.col("o.customer_id"))
    .agg(F.sum(F.col("s.cost")).alias("total_cost"))
    .filter(F.col("total_cost") >= 170)
)
