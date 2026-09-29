from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_read_at_month_by_rea = Window.partitionBy("quality", F.month("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_kwh_prior = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(-6, -1)
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy("kwh", "id")
w_quality_read_at_month_by_rea_2 = Window.partitionBy("quality", F.month("read_at")).orderBy(F.col("read_at").desc())
w_quality_read_at_month_by_rea_3 = Window.partitionBy("quality", F.month("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_site_id_by_kwh_prior_2 = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_quality_site_id_by_kwh_2 = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc_nulls_first(), "id")
w_quality_site_id_by_kwh_runni = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn = Window.partitionBy("rn")
w_kwh_quartile_kwh_seq_by_kwh = Window.partitionBy("kwh_quartile", "kwh_seq").orderBy("kwh_seq", "id")
w_kwh_quartile_quality_by_kwh = Window.partitionBy("kwh_quartile", "quality").orderBy("kwh_seq", "id")
w_kwh_dense_rank_site_id_by_rn = Window.partitionBy("kwh_dense_rank", "site_id").orderBy(F.col("rn").desc(), "id").rowsBetween(-6, Window.currentRow)
w_kwh_quartile_kwh_seq_by_kwh_2 = Window.partitionBy("kwh_quartile", "kwh_seq").orderBy("kwh_seq")

recent_readings = (
    df_meter_readings
    .filter((F.col("site_id") >= 8) & (F.col("quality").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

metrics = (
    recent_readings
    .select(
        "*",
        F.first("kwh").over(w_quality_read_at_month_by_rea).alias("first_kwh"),
        F.max("kwh").over(w_quality_site_id_by_kwh_prior).alias("kwh_max_6_prior"),
        F.row_number().over(w_quality_site_id_by_kwh).alias("rn"),
        F.round(F.cume_dist().over(w_quality_read_at_month_by_rea_2), 3).alias("read_at_cume_dist"),
        F.round(F.avg("kwh").over(w_quality_read_at_month_by_rea_3), 2).alias("kwh_avg_2"),
        F.dense_rank().over(w_quality_site_id_by_kwh).alias("kwh_dense_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_id), F.lit(0)), 4).alias("kwh_share_of_quality_site_id"),
        F.max("kwh").over(w_quality_site_id_by_kwh_prior_2).alias("kwh_max_1_prior"),
        F.ntile(4).over(w_quality_site_id_by_kwh).alias("kwh_quartile"),
        F.dense_rank().over(w_quality_read_at_month_by_rea_2).alias("read_at_dense_rank"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.row_number().over(w_quality_site_id_by_kwh_2).alias("kwh_seq"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_site_id_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.count(F.lit(1)).over(w_quality_site_id_by_kwh_runni).alias("running_count"),
        F.nth_value("kwh", 2).over(w_quality_site_id_by_kwh_full).alias("second_kwh"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "first_kwh",
        "kwh_max_6_prior",
        "rn",
        "kwh_avg_2",
        "kwh_max_1_prior",
        "kwh_quartile",
        "rows_in_quality_site_id",
        "kwh_seq",
        "minutes_since_prev_read_at",
        "running_count",
        F.round(F.avg("kwh_avg_2").over(w_rn), 2).alias("rn_avg_kwh_avg_2"),
        (F.col("kwh_max_6_prior") - F.lag("kwh_max_6_prior").over(w_kwh_quartile_kwh_seq_by_kwh)).alias("kwh_max_6_prior_change"),
        (F.col("kwh_dense_rank") - F.lag("kwh_dense_rank").over(w_kwh_quartile_quality_by_kwh)).alias("kwh_dense_rank_change"),
        F.round(F.avg("kwh_max_1_prior").over(w_kwh_dense_rank_site_id_by_rn), 2).alias("kwh_max_1_prior_avg_7"),
        F.row_number().over(w_kwh_quartile_quality_by_kwh).alias("kwh_seq_seq"),
        F.dense_rank().over(w_kwh_quartile_kwh_seq_by_kwh_2).alias("kwh_seq_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "kwh",
        "first_kwh",
        "rn",
        "kwh_avg_2",
        "kwh_max_1_prior",
        "kwh_quartile",
        "rows_in_quality_site_id",
        "minutes_since_prev_read_at",
        "running_count",
        "rn_avg_kwh_avg_2",
        "kwh_max_6_prior_change",
        "kwh_dense_rank_change",
        "kwh_max_1_prior_avg_7",
        "kwh_seq_seq",
        "kwh_seq_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
    .orderBy("site_id", "kwh_seq_dense_rank", "id")
    .limit(5)
)
