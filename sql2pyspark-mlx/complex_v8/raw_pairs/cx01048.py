from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-08-03").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.date_add(F.col("end_date"), 90).alias("end_date_plus_90d"),
        F.abs(F.col("premium") - 2096.6).alias("premium_distance"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

status_stats = (
    prepared
    .groupBy("status")
    .agg(F.avg("premium_distance").alias("avg_premium_distance"))
)

above_average = (
    prepared.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium_distance") > F.col("g.avg_premium_distance"))
    .select(
        "s.*",
        "g.avg_premium_distance",
        F.round(F.col("s.premium_distance") - F.col("g.avg_premium_distance"), 2).alias("premium_distance_minus_avg"),
    )
)

enriched = (
    above_average.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.premium_distance",
        "s.status_clean",
        "s.avg_premium_distance",
        "s.premium_distance_minus_avg",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

enriched_2 = (
    enriched
    .filter(F.col("premium_distance_minus_avg") > -1000.0)
    .select(
        "id",
        "holder_id",
        "premium_distance",
        "status_clean",
        "holder_joined_on",
        "holder_segment",
        "holder_name",
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

result = (
    enriched_2
    .groupingSets(
        [["holder_id", "holder_segment"], ["holder_id"], ["holder_segment"]],
        "holder_id", "holder_segment",
    )
    .agg(F.array_sort(F.collect_set("holder_name")).alias("holder_name_list"))
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
