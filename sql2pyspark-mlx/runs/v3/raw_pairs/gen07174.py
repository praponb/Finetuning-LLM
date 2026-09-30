from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("channel").isNull())
    .groupBy("store_id")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.city"), F.col("st.channel_list"), F.col("st.status_list"))
    .orderBy("city")
)
