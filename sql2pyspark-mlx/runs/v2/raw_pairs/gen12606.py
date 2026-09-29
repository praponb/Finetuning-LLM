from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.cancelled_count") > 23)
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("st.cancelled_count"),
        F.col("st.status_values"),
    )
)
