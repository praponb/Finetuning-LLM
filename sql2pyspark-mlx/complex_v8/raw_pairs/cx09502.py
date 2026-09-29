from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_segment_status_by_sta = Window.partitionBy("holder_segment", "status").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_segment_by_start_date = Window.partitionBy("holder_segment").orderBy(F.col("start_date").desc_nulls_first(), "id")
w_holder_segment_status_by_sta_2 = Window.partitionBy("holder_segment", "status").orderBy(F.col("start_date").desc(), "id")
w_all_by_start_date = Window.orderBy(F.col("start_date").asc_nulls_last(), "id")
w_holder_segment_by_start_date_2 = Window.partitionBy("holder_segment").orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next2_start_date_month_rn = Window.partitionBy(F.month("next2_start_date"), "rn")
w_rn_by_start_date = Window.partitionBy("rn").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_rn_by_start_date_prior_3 = Window.partitionBy("rn").orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_first_start_date_ties = Window.orderBy(F.col("first_start_date").desc_nulls_first())
w_distinct_holder_segments_by = Window.partitionBy("distinct_holder_segments").orderBy("avg_distinct_holder_segment_in_next2_start_date_", "first_start_date_rank", "distinct_holder_segment_in_next2_start_date_mont").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_distinct_holder_segme = Window.partitionBy("n_rows", "distinct_holder_segments")

prepared = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

with_parent_holder = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
    )
)

windowed = (
    with_parent_holder
    .select(
        "*",
        F.first("start_date").over(w_holder_segment_status_by_sta).alias("first_start_date"),
        F.lag("start_date").over(w_holder_segment_by_start_date).alias("prev_start_date"),
        F.row_number().over(w_holder_segment_status_by_sta_2).alias("rn"),
        F.round(F.percent_rank().over(w_holder_segment_by_start_date), 3).alias("start_date_percent_rank"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_start_date)).alias("days_since_prev_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_segment_by_start_date)).alias("days_since_prev_start_date_3"),
        F.lead("start_date", 2).over(w_holder_segment_status_by_sta_2).alias("next2_start_date"),
        F.last("start_date", ignorenulls=True).over(w_holder_segment_by_start_date_2).alias("last_known_start_date"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        "holder_segment",
        "prev_start_date",
        "rn",
        "days_since_prev_start_date",
        "days_since_prev_start_date_3",
        "next2_start_date",
        F.round(F.col("rn") / F.nullif(F.sum("rn").over(w_next2_start_date_month_rn), F.lit(0)), 4).alias("rn_share_of_next2_start_date_month_rn"),
        F.round(F.col("days_since_prev_start_date") / F.nullif(F.sum("days_since_prev_start_date").over(w_next2_start_date_month_rn), F.lit(0)), 4).alias("days_since_prev_start_date_share_of_next2_start_"),
        F.datediff(F.col("first_start_date"), F.lag("first_start_date").over(w_rn_by_start_date)).alias("days_since_prev_first_start_date"),
        F.size(F.collect_set("holder_segment").over(w_next2_start_date_month_rn)).alias("distinct_holder_segment_in_next2_start_date_mont"),
        F.sum("days_since_prev_start_date").over(w_rn_by_start_date_prior_3).alias("days_since_prev_start_date_sum_3_prior"),
        F.rank().over(w_all_by_first_start_date_ties).alias("first_start_date_rank"),
        F.size(F.collect_set("status").over(w_next2_start_date_month_rn)).alias("distinct_status_in_next2_start_date_month_rn"),
    )
)

rollup_base = (
    sequenced
    .groupBy(
        "first_start_date_rank",
        "distinct_holder_segment_in_next2_start_date_mont",
    )
    .agg(
        F.array_sort(F.collect_set("holder_segment")).alias("holder_segment_list"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("distinct_holder_segment_in_next2_start_date_mont"), 2).alias("avg_distinct_holder_segment_in_next2_start_date_"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.countDistinct("holder_segment").alias("distinct_holder_segments"),
    )
)

ranked = (
    rollup_base
    .withColumn("first_distinct_holder_segments", F.first("distinct_holder_segments").over(w_distinct_holder_segments_by))
    .withColumn("n_rows_distinct_holder_segments_min_distinct_hol", F.min("distinct_holder_ids").over(w_n_rows_distinct_holder_segme))
)

result = (
    ranked
    .select(
        "first_start_date_rank",
        "distinct_holder_segment_in_next2_start_date_mont",
        "n_rows",
        "avg_distinct_holder_segment_in_next2_start_date_",
        "distinct_holder_ids",
        "distinct_holder_segments",
        "first_distinct_holder_segments",
        (F.floor(F.col("first_start_date_rank") / 2) * 2).alias("first_start_date_rank_bucket"),
    )
)
