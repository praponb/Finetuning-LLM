from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("status").isin("ACTIVE", "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.date_add(F.col("start_date"), 30).alias("start_date_plus_30d"),
    )
)

joined_holder = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.upper(F.trim(F.col("p.holder_name"))).like("G%"))
    .select("s.*", "p.holder_name")
)

filtered = (
    joined_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date").isNotNull()) & (~F.col("p.status").isin("CANCELLED", "LAPSED")), "left_semi")
)

holder_id_stats = (
    filtered
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_premium"))
)

above_average = (
    filtered.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium") == F.col("g.max_premium"))
    .select(
        "s.*",
        "g.max_premium",
        F.round(F.col("s.premium") - F.col("g.max_premium"), 2).alias("premium_minus_max"),
    )
)

result = (
    above_average
    .cube("status", "product")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.datediff(F.max("start_date_plus_30d"), F.min("start_date_plus_30d")).alias("start_date_plus_30d_span_days"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("product").asc_nulls_last())
)
