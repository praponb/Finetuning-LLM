from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_end_date = Window.partitionBy("status").orderBy("end_date", "id")
w_all_by_premium_bucket = Window.orderBy(F.col("premium_bucket").desc(), "id")
w_holder_segment = Window.partitionBy("holder_segment")
w_all_by_premium_bucket_full = Window.orderBy(F.col("premium_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date_running = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_end_date_prior_6 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-6, -1)
w_all_by_start_date_full = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_row_count_by_row_count_prior = Window.partitionBy("row_count").orderBy(F.col("row_count").desc(), "holder_segment").rowsBetween(-6, -1)
w_row_count_by_row_count_movin = Window.partitionBy("row_count").orderBy(F.col("row_count").desc(), "holder_segment").rowsBetween(-2, Window.currentRow)

scoped_policys = (
    df_policies
    .filter((F.col("status").isNotNull()) & (F.col("start_date") >= F.lit("2024-07-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

joined = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_bucket",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "end_date",
        "status",
        "premium_bucket",
        "holder_joined_on",
        "holder_risk_score",
        "holder_segment",
        F.lag("start_date").over(w_status_by_end_date).alias("prev_start_date"),
        F.row_number().over(w_all_by_premium_bucket).alias("rn"),
        F.round(F.col("premium_bucket") - F.avg("premium_bucket").over(w_holder_segment), 2).alias("premium_bucket_vs_holder_segment_avg"),
        F.min("premium_bucket").over(w_holder_segment).alias("holder_segment_min_premium_bucket"),
        F.last("start_date").over(w_all_by_premium_bucket_full).alias("last_start_date"),
        F.lag("premium_bucket", 1, 0).over(w_all_by_premium_bucket).alias("prev_premium_bucket"),
        F.min("holder_risk_score").over(w_status_by_end_date_running).alias("min_holder_risk_score_so_far"),
        F.max("premium_bucket").over(w_status_by_end_date_prior_6).alias("premium_bucket_max_6_prior"),
        F.last("premium_bucket").over(w_all_by_start_date_full).alias("last_premium_bucket"),
        F.lag("holder_risk_score", 3).over(w_all_by_premium_bucket).alias("prev3_holder_risk_score"),
        (F.col("holder_risk_score") - F.lag("holder_risk_score").over(w_status_by_end_date)).alias("holder_risk_score_change"),
        F.ntile(10).over(w_all_by_start_date).alias("start_date_decile"),
    )
)

leaders = (
    with_windows
    .filter(F.col("rn") <= 5)
    .drop("rn")
)

rollup_base = (
    leaders
    .groupBy("holder_segment")
    .agg(
        F.round(F.sum("prev3_holder_risk_score") / F.nullif(F.sum("prev_premium_bucket"), F.lit(0)), 3).alias("prev3_holder_risk_score_to_prev_premium_bucket_r"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("row_count"),
    )
)

with_windows_2 = (
    rollup_base
    .withColumn("row_count_sum_6_prior", F.sum("row_count").over(w_row_count_by_row_count_prior))
    .withColumn("row_count_sum_3", F.sum("row_count").over(w_row_count_by_row_count_movin))
)

result = (
    with_windows_2
    .filter(F.col("row_count_sum_6_prior") <= 27)
    .select(
        "holder_segment",
        "prev3_holder_risk_score_to_prev_premium_bucket_r",
        "status_list",
        "row_count",
        "row_count_sum_3",
        (F.floor(F.col("row_count_sum_3") / 5) * 5).alias("row_count_sum_3_bucket"),
        F.abs(F.col("row_count_sum_6_prior") - 18).alias("row_count_sum_6_prior_distance"),
    )
    .orderBy("row_count_sum_3", "holder_segment")
    .limit(5)
)
