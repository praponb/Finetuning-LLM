from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_by_days_since_joined = Window.partitionBy("region").orderBy("days_since_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_segment = Window.partitionBy("segment")
w_row_count_segment = Window.partitionBy("row_count", "segment")
w_segment_row_count_by_median = Window.partitionBy("segment", "row_count").orderBy(F.col("median_first_risk_score_distance").desc(), "segment").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_holders = (
    df_policyholders
    .filter(F.col("joined_on").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
        F.substring(F.trim(F.col("segment")), 1, 4).alias("segment_prefix"),
    )
)

metrics = (
    filtered_holders
    .select(
        "*",
        F.first("risk_score").over(w_region_by_days_since_joined).alias("first_risk_score"),
        F.last("risk_score").over(w_region_by_days_since_joined).alias("last_risk_score"),
        F.round(F.col("days_since_joined_on") - F.avg("days_since_joined_on").over(w_segment), 2).alias("days_since_joined_on_vs_segment_avg"),
    )
)

labeled = (
    metrics
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        "segment_ref",
        "first_risk_score",
        F.abs(F.col("first_risk_score") - 0.3).alias("first_risk_score_distance"),
        F.substring(F.trim(F.col("segment_ref")), 1, 2).alias("segment_ref_prefix"),
        F.concat(F.col("holder_name"), F.lit("#"), F.col("days_since_joined_on").cast("string")).alias("holder_name_ref"),
    )
)

stats = (
    labeled
    .groupBy("segment")
    .agg(
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("segment").isNotNull(), 1)) / F.count("*"), 1).alias("pct_segment_is_not_null"),
        F.percentile_approx("first_risk_score_distance", 0.5).alias("median_first_risk_score_distance"),
        F.array_sort(F.collect_set("segment")).alias("segment_list"),
    )
)

with_windows = (
    stats
    .select(
        "*",
        F.round(F.avg("pct_segment_is_not_null").over(w_row_count_segment), 2).alias("row_count_segment_avg_pct_segment_is_not_null"),
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_row_count_segment), F.lit(0)), 4).alias("row_count_share_of_row_count_segment"),
        F.count(F.lit(1)).over(w_row_count_segment).alias("rows_in_row_count_segment"),
        F.nth_value("pct_segment_is_not_null", 2).over(w_segment_row_count_by_median).alias("second_pct_segment_is_not_null"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["row_count", "segment"], ["row_count"], ["segment"]],
        "row_count", "segment",
    )
    .agg(
        F.round(F.avg("row_count"), 2).alias("avg_row_count"),
        F.count("*").alias("row_count_2"),
    )
)
