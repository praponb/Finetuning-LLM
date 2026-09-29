from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_end_date_ties = Window.partitionBy("status").orderBy(F.col("end_date").desc())
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_holder_id_holder_joined_on_y = Window.partitionBy("holder_id", F.year("holder_joined_on"))
w_status_by_holder_joined_on_f = Window.partitionBy("status").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_year_holder_id_by_h = Window.partitionBy(F.year("end_date"), "holder_id").orderBy(F.col("holder_joined_on").desc())
w_status_by_premium_distance_2 = Window.partitionBy("status").orderBy(F.col("premium_distance_2").desc_nulls_first(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_status_by_holder_joined_on = Window.partitionBy("status").orderBy("holder_joined_on", "id")
w_status_by_holder_joined_on_f_2 = Window.partitionBy("status").orderBy(F.col("holder_joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_year_holder_id_by_h_2 = Window.partitionBy(F.year("end_date"), "holder_id").orderBy(F.col("holder_joined_on").desc(), "id")
w_status_by_holder_joined_on_m = Window.partitionBy("status").orderBy("holder_joined_on", "id").rowsBetween(-2, Window.currentRow)
w_last_holder_joined_on_month = Window.partitionBy(F.month("last_holder_joined_on")).orderBy("holder_id_holder_joined_on_year_min_premium_dist", "id")
w_prev_holder_joined_on_year_e = Window.partitionBy(F.year("prev_holder_joined_on"), "end_date_dense_rank")
w_last_holder_joined_on_month_2 = Window.partitionBy(F.month("last_holder_joined_on")).orderBy("holder_id_holder_joined_on_year_min_premium_dist", "id").rowsBetween(-1, -1)
w_seq_no_by_premium_distance_2 = Window.partitionBy("seq_no").orderBy("premium_distance_2_cume_dist")
w_all_by_premium_distance_2_mo = Window.orderBy(F.col("premium_distance_2").desc(), "id").rowsBetween(-2, Window.currentRow)

prepared_policys = (
    df_policies
    .filter(~F.col("status").isin("LAPSED", "CANCELLED"))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.abs(F.col("premium") - 3532.7).alias("premium_distance"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance_2"),
    )
)

enriched = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.premium_distance_2",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        "premium_distance_2",
        "holder_joined_on",
        F.dense_rank().over(w_status_by_end_date_ties).alias("end_date_dense_rank"),
        F.max("premium_distance_2").over(w_holder_id_holder_joined_on_y).alias("holder_id_holder_joined_on_year_max_premium_dist"),
        F.last("holder_joined_on").over(w_status_by_holder_joined_on_f).alias("last_holder_joined_on"),
        F.round(F.percent_rank().over(w_end_date_year_holder_id_by_h), 3).alias("holder_joined_on_percent_rank"),
        F.rank().over(w_end_date_year_holder_id_by_h).alias("holder_joined_on_rank"),
        F.dense_rank().over(w_end_date_year_holder_id_by_h).alias("holder_joined_on_dense_rank"),
        F.round(F.cume_dist().over(w_status_by_premium_distance_2), 3).alias("premium_distance_2_cume_dist"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.count(F.lit(1)).over(w_holder_id_holder_joined_on_y).alias("rows_in_holder_id_holder_joined_on_year"),
        F.row_number().over(w_status_by_holder_joined_on).alias("holder_joined_on_seq"),
        F.first("premium_distance_2").over(w_status_by_holder_joined_on_f_2).alias("first_premium_distance_2"),
        F.row_number().over(w_end_date_year_holder_id_by_h_2).alias("seq_no"),
        F.lag("holder_joined_on").over(w_status_by_holder_joined_on).alias("prev_holder_joined_on"),
        F.lag("holder_joined_on").over(w_status_by_premium_distance_2).alias("prev_holder_joined_on_2"),
        F.round(F.avg("premium_distance_2").over(w_status_by_holder_joined_on_m), 2).alias("premium_distance_2_avg_3"),
        F.min("premium_distance_2").over(w_holder_id_holder_joined_on_y).alias("holder_id_holder_joined_on_year_min_premium_dist"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.concat(F.col("status"), F.lit("#"), F.col("end_date_dense_rank").cast("string")).alias("status_ref"),
        F.round(F.col("seq_no") / F.nullif(F.col("holder_joined_on_seq"), F.lit(0)), 3).alias("seq_no_per_holder_joined_on_seq"),
        F.last_day(F.col("prev_holder_joined_on_2")).alias("prev_holder_joined_on_2_month_end"),
    )
)

windowed = (
    labeled
    .select(
        "id",
        "holder_id",
        "status",
        "premium_distance_2",
        "holder_joined_on",
        "end_date_dense_rank",
        "holder_id_holder_joined_on_year_max_premium_dist",
        "last_holder_joined_on",
        "holder_joined_on_percent_rank",
        "holder_joined_on_rank",
        "holder_joined_on_dense_rank",
        "premium_distance_2_cume_dist",
        "rows_in_holder_id",
        "rows_in_holder_id_holder_joined_on_year",
        "holder_joined_on_seq",
        "prev_holder_joined_on",
        "premium_distance_2_avg_3",
        "holder_id_holder_joined_on_year_min_premium_dist",
        "prev_holder_joined_on_2_month_end",
        F.round(F.percent_rank().over(w_last_holder_joined_on_month), 3).alias("holder_id_holder_joined_on_year_min_premium_dist_2"),
        F.round(F.avg("holder_joined_on_rank").over(w_prev_holder_joined_on_year_e), 2).alias("prev_holder_joined_on_year_end_date_dense_rank_a"),
        F.round(F.avg("premium_distance_2_cume_dist").over(w_last_holder_joined_on_month_2), 2).alias("premium_distance_2_cume_dist_avg_1_prior"),
        F.rank().over(w_seq_no_by_premium_distance_2).alias("premium_distance_2_cume_dist_rank"),
        F.rank().over(w_last_holder_joined_on_month).alias("holder_id_holder_joined_on_year_min_premium_dist_3"),
        F.round(F.avg("holder_joined_on_dense_rank").over(w_all_by_premium_distance_2_mo), 2).alias("holder_joined_on_dense_rank_avg_3"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "status",
        "holder_joined_on",
        "end_date_dense_rank",
        "holder_id_holder_joined_on_year_max_premium_dist",
        "last_holder_joined_on",
        "holder_joined_on_percent_rank",
        "holder_joined_on_dense_rank",
        "rows_in_holder_id",
        "prev_holder_joined_on",
        "holder_id_holder_joined_on_year_min_premium_dist",
        "prev_holder_joined_on_2_month_end",
        "holder_id_holder_joined_on_year_min_premium_dist_2",
        "prev_holder_joined_on_year_end_date_dense_rank_a",
        "premium_distance_2_cume_dist_rank",
        "holder_id_holder_joined_on_year_min_premium_dist_3",
        "holder_joined_on_dense_rank_avg_3",
        F.datediff(F.col("prev_holder_joined_on"), F.col("last_holder_joined_on")).alias("days_last_holder_joined_on_to_prev_holder_joined"),
        (F.floor(F.col("holder_joined_on_rank") / 2) * 2).alias("holder_joined_on_rank_bucket"),
    )
    .orderBy(F.col("premium_distance_2_cume_dist_rank").desc())
)
