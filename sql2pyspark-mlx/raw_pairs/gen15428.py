from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("amount") < 810)
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("st.status_values"),
        F.col("st.stddev_discount"),
    )
)
