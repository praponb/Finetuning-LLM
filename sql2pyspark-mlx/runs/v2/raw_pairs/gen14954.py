from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "right")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("o.store_id"),
        F.col("st.region"),
        F.add_months(F.col("s.delivered_date"), 6).alias("delivered_date_plus_months"),
    )
    .orderBy(F.col("store_id").desc(), F.col("cost").desc())
)
