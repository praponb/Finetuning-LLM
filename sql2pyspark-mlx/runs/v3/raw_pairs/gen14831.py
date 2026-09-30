from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("o.discount").between(0.32, 0.38))
    .select(
        F.col("s.shipped_date"),
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("st.city"),
        F.last_day(F.col("s.delivered_date")).alias("delivered_date_month_end"),
    )
    .orderBy("delivered_date_month_end")
)
