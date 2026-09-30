from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("S.channel_values"))
    .orderBy(F.col("store_name").desc())
)
