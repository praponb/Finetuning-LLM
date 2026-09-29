from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .groupBy("store_id")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.completed_count") > 32)
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("st.latest_order_date"),
        F.col("st.completed_count"),
    )
    .orderBy(F.col("completed_count").desc(), "store_name")
)
