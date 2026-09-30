from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("discount") < 0.38)
    .groupBy("store_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("st.min_order_date"))
    .orderBy("id")
)
