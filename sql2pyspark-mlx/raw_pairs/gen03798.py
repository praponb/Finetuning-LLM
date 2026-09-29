from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.channel_values"),
        F.col("st.partner_count"),
    )
    .orderBy(F.col("city").desc())
)
