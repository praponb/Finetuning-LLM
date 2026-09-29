from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
        F.col("o.customer_id"),
    )
    .agg(F.sum(F.col("s.cost")).alias("total_cost"))
    .filter(F.col("total_cost") >= 460)
    .orderBy(F.col("customer_id").desc(), "order_month")
    .limit(5)
)
