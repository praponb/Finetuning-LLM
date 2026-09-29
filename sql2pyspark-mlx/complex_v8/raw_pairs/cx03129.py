from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_or_unknown_2 = Window.partitionBy("status_or_unknown_2")
w_distinct_holder_regions_dist = Window.partitionBy("distinct_holder_regions_distance").orderBy(F.col("max_premium").desc(), "status_or_unknown_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_holder_regions_dist_2 = Window.partitionBy("distinct_holder_regions_distance").orderBy(F.col("max_premium").desc(), "status_or_unknown_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") <= 15) & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown_2"),
    )
)

enriched_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.status_or_unknown",
        "s.status_or_unknown_2",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

without_matches = (
    enriched_holder.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region").isNotNull()) & (F.lower(F.col("p.holder_name")).like("%er%")), "left_semi")
)

summary = (
    without_matches
    .groupBy("status_or_unknown_2")
    .agg(
        F.round(F.sum("premium") / F.nullif(F.sum("holder_risk_score"), F.lit(0)), 3).alias("premium_to_holder_risk_score_ratio"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.countDistinct("holder_region").alias("distinct_holder_regions"),
        F.max("premium").alias("max_premium"),
    )
)

tagged = (
    summary
    .select(
        "status_or_unknown_2",
        "total_premium",
        "distinct_holder_regions",
        "max_premium",
        F.initcap(F.trim(F.col("status_or_unknown_2"))).alias("status_or_unknown_2_clean"),
        F.abs(F.col("distinct_holder_regions") - 6).alias("distinct_holder_regions_distance"),
        F.greatest(F.col("total_premium"), F.col("max_premium")).alias("greatest_total_premium_max_premium"),
    )
)

without_matches_2 = (
    tagged
    .crossJoin(tagged.agg(F.min("max_premium").alias("overall_min_max_premium")))
    .filter(F.col("max_premium") > F.col("overall_min_max_premium"))
    .drop("overall_min_max_premium")
)

with_windows = (
    without_matches_2
    .select(
        "*",
        F.max("distinct_holder_regions").over(w_status_or_unknown_2).alias("status_or_unknown_2_max_distinct_holder_regions"),
        F.round(F.col("max_premium") - F.avg("max_premium").over(w_status_or_unknown_2), 2).alias("max_premium_vs_status_or_unknown_2_avg"),
        F.first("distinct_holder_regions").over(w_distinct_holder_regions_dist).alias("first_distinct_holder_regions"),
        F.round(F.col("distinct_holder_regions") - F.avg("distinct_holder_regions").over(w_status_or_unknown_2), 2).alias("distinct_holder_regions_vs_status_or_unknown_2_a"),
        F.first("max_premium", ignorenulls=True).over(w_distinct_holder_regions_dist_2).alias("first_known_max_premium"),
        F.round(F.avg("total_premium").over(w_status_or_unknown_2), 2).alias("status_or_unknown_2_avg_total_premium"),
    )
)

result = (
    with_windows
    .select(
        "status_or_unknown_2",
        "total_premium",
        "max_premium",
        "status_or_unknown_2_clean",
        "distinct_holder_regions_distance",
        "status_or_unknown_2_max_distinct_holder_regions",
        "max_premium_vs_status_or_unknown_2_avg",
        "first_distinct_holder_regions",
        "distinct_holder_regions_vs_status_or_unknown_2_a",
        "status_or_unknown_2_avg_total_premium",
        F.coalesce(F.col("status_or_unknown_2"), F.lit("UNKNOWN")).alias("status_or_unknown_2_or_unknown"),
        F.greatest(F.col("total_premium"), F.col("max_premium")).alias("greatest_total_premium_max_premium_2"),
    )
    .orderBy(
        F.col("distinct_holder_regions_vs_status_or_unknown_2_a").desc_nulls_first(),
        F.col("total_premium").desc(),
        "status_or_unknown_2",
    )
    .limit(20)
)
