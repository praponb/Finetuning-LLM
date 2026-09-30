from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.max("discount").alias("max_discount"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.avg_discount") >= 70)
    .select(F.col("s.opened_date"), F.col("st.max_discount"), F.col("st.avg_discount"))
    .orderBy("avg_discount")
)
