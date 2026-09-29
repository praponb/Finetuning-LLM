from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

qualified = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") >= 4868.7) & (F.col("c.adjuster_id").isNotNull()), "left_anti")
)

labeled = (
    qualified
    .select(
        "*",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.month(F.col("end_date")).alias("end_date_month"),
        F.coalesce(F.col("premium_filled"), F.lit(0.0)).alias("premium_filled_filled"),
    )
)

enriched = (
    labeled
    .select(
        "*",
        F.round(F.col("end_date_month") / F.nullif(F.col("premium_filled"), F.lit(0)), 3).alias("end_date_month_per_premium_filled"),
    )
)

holder_id_stats = (
    enriched
    .groupBy("holder_id")
    .agg(F.avg("end_date_month").alias("avg_end_date_month"))
)

outliers = (
    enriched.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.end_date_month") > F.col("g.avg_end_date_month"))
    .select(
        "s.*",
        "g.avg_end_date_month",
        F.round(F.col("s.end_date_month") - F.col("g.avg_end_date_month"), 2).alias("end_date_month_minus_avg"),
    )
)

result = (
    outliers
    .groupingSets(
        [["status", "end_date_month"], ["status"], []],
        "status", "end_date_month",
    )
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count(F.when(~F.col("status").isin("ACTIVE", "EXPIRED"), 1)).alias("n_status_not_in_active_expired"),
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("end_date_month").asc_nulls_last(),
    )
)
