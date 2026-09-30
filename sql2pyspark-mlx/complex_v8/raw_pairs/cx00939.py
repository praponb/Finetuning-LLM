from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment = Window.partitionBy("segment")
w_all_by_risk_score_running = Window.orderBy(F.col("risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_risk_score = Window.orderBy(F.col("risk_score").desc(), "id")
w_segment_by_joined_on_running = Window.partitionBy("segment").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_risk_score_full = Window.orderBy(F.col("risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_joined_on_month_segme = Window.partitionBy(F.month("second_joined_on"), "segment").orderBy(F.col("greatest_max_risk_score_so_far_segment_max_risk_").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_joined_on_month = Window.partitionBy(F.month("second_joined_on"))
w_first_known_joined_on_month = Window.partitionBy(F.date_trunc("month", "first_known_joined_on")).orderBy("segment_avg_risk_score", "id")
w_n_segment_sme_joined_on_year = Window.partitionBy("n_segment_sme", "joined_on_year")
w_n_segment_sme_2_by_n_segment = Window.partitionBy("n_segment_sme_2").orderBy(F.col("n_segment_sme_2").asc_nulls_last(), "segment", "joined_on_year")

scoped = (
    df_policyholders
    .filter(F.col("segment") != "VIP")
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_segment), 2).alias("risk_score_vs_segment_avg"),
        F.max("risk_score").over(w_all_by_risk_score_running).alias("max_risk_score_so_far"),
        F.datediff(F.col("joined_on"), F.lag("joined_on").over(w_all_by_risk_score)).alias("days_since_prev_joined_on"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_segment), F.lit(0)), 4).alias("risk_score_share_of_segment"),
        F.max("risk_score").over(w_segment_by_joined_on_running).alias("max_risk_score_so_far_2"),
        F.max("risk_score").over(w_segment).alias("segment_max_risk_score"),
        F.first("joined_on", ignorenulls=True).over(w_segment_by_joined_on_running).alias("first_known_joined_on"),
        F.nth_value("joined_on", 2).over(w_all_by_risk_score_full).alias("second_joined_on"),
        F.round(F.avg("risk_score").over(w_segment), 2).alias("segment_avg_risk_score"),
        F.count(F.lit(1)).over(w_segment).alias("rows_in_segment"),
    )
)

annotated = (
    ranked
    .filter(F.col("risk_score_share_of_segment") <= 0.6)
    .select(
        "*",
        F.greatest(F.col("max_risk_score_so_far"), F.col("segment_max_risk_score")).alias("greatest_max_risk_score_so_far_segment_max_risk_"),
        F.greatest(F.col("segment_avg_risk_score"), F.col("segment_max_risk_score")).alias("greatest_segment_avg_risk_score_segment_max_risk"),
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
    )
)

windowed = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_segment).alias("rows_in_segment_2"),
        F.last("max_risk_score_so_far").over(w_second_joined_on_month_segme).alias("last_max_risk_score_so_far"),
        F.count(F.lit(1)).over(w_second_joined_on_month).alias("rows_in_second_joined_on_month"),
        F.row_number().over(w_first_known_joined_on_month).alias("segment_avg_risk_score_seq"),
        F.sum("days_since_prev_joined_on").over(w_segment).alias("segment_sum_days_since_prev_joined_on"),
    )
)

stats = (
    windowed
    .filter(F.col("risk_score").between(0.2, 0.9))
    .groupBy("segment", F.year("joined_on").alias("joined_on_year"))
    .agg(
        F.sum(F.when(F.col("segment") == "SME", 1).otherwise(0)).alias("n_segment_sme"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.sum(F.when(F.col("segment") != "SME", 1).otherwise(0)).alias("n_segment_sme_2"),
        F.count("*").alias("row_count"),
        F.min("joined_on").alias("first_joined_on"),
    )
)

with_windows = (
    stats
    .select(
        "segment",
        "joined_on_year",
        "n_segment_sme",
        "distinct_segments",
        "n_segment_sme_2",
        "row_count",
        F.round(F.avg("row_count").over(w_n_segment_sme_joined_on_year), 2).alias("n_segment_sme_joined_on_year_avg_row_count"),
        F.row_number().over(w_n_segment_sme_2_by_n_segment).alias("n_segment_sme_2_seq"),
        F.lag("n_segment_sme").over(w_n_segment_sme_2_by_n_segment).alias("prev_n_segment_sme"),
    )
)

result = (
    with_windows
    .filter(F.col("prev_n_segment_sme") <= 9)
    .select(
        "segment",
        "joined_on_year",
        "distinct_segments",
        "row_count",
        "n_segment_sme_joined_on_year_avg_row_count",
        "n_segment_sme_2_seq",
        "prev_n_segment_sme",
        F.length(F.trim(F.col("segment"))).alias("segment_len"),
        F.abs(F.col("joined_on_year") - 2025).alias("joined_on_year_distance"),
    )
    .orderBy("prev_n_segment_sme")
)
