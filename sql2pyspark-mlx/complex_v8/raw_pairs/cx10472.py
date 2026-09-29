from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_week_month_read_at_m = Window.partitionBy(F.date_trunc("month", "read_at_week"), F.date_trunc("month", "read_at")).orderBy("read_at_week", "id").rowsBetween(-2, -1)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_read_at_month_quality = Window.partitionBy(F.month("read_at"), "quality")
w_site_id_read_at_week_year_by = Window.partitionBy("site_id", F.year("read_at_week")).orderBy(F.col("days_since_read_at").desc(), "id")
w_read_at_week_year_quality = Window.partitionBy(F.year("read_at_week"), "quality")
w_read_at_to_date_quality_by_r = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_days_since_read_a = Window.partitionBy("quality").orderBy("days_since_read_at")
w_read_at_to_date_quality_by_r_2 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("read_at", "id").rowsBetween(-1, -1)
w_read_at_to_date_quality_by_r_3 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("read_at", "id")
w_quality_by_days_since_read_a_2 = Window.partitionBy("quality").orderBy("days_since_read_at", "id")
w_read_at_to_date_quality_by_r_4 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_site_id_read_at_week_year_by_2 = Window.partitionBy("site_id", F.year("read_at_week")).orderBy(F.col("days_since_read_at_percent_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_days_since_read_at_rank_site = Window.partitionBy("days_since_read_at_rank", "site_id").orderBy("kwh_sum_1_prior", "id")
w_days_since_read_at_rank_site_2 = Window.partitionBy("days_since_read_at_rank", "site_id").orderBy("kwh_sum_1_prior")
w_site_id_read_at_week_year_by_3 = Window.partitionBy("site_id", F.year("read_at_week")).orderBy(F.col("days_since_read_at_percent_rank").asc_nulls_last(), "id")

cleaned = (
    df_meter_readings
    .filter((~F.col("quality").isin("EST", "OK")) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "site_id",
        "quality",
        "read_at_week",
        F.sum("days_since_read_at").over(w_read_at_week_month_read_at_m).alias("days_since_read_at_sum_2_prior"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.count(F.lit(1)).over(w_read_at_month_quality).alias("rows_in_read_at_month_quality"),
        F.row_number().over(w_site_id_read_at_week_year_by).alias("days_since_read_at_seq"),
        F.round(F.col("days_since_read_at") - F.avg("days_since_read_at").over(w_read_at_month_quality), 2).alias("days_since_read_at_vs_read_at_month_quality_avg"),
        F.count(F.lit(1)).over(w_read_at_week_year_quality).alias("rows_in_read_at_week_year_quality"),
        F.round(F.col("days_since_read_at") / F.nullif(F.sum("days_since_read_at").over(w_read_at_week_year_quality), F.lit(0)), 4).alias("days_since_read_at_share_of_read_at_week_year_qu"),
        F.first("read_at_week", ignorenulls=True).over(w_read_at_to_date_quality_by_r).alias("first_known_read_at_week"),
        F.round(F.percent_rank().over(w_quality_by_days_since_read_a), 3).alias("days_since_read_at_percent_rank"),
        F.sum("kwh").over(w_read_at_to_date_quality_by_r_2).alias("kwh_sum_1_prior"),
        F.row_number().over(w_read_at_to_date_quality_by_r_3).alias("read_at_seq"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_by_days_since_read_a_2)).alias("kwh_change"),
        F.rank().over(w_quality_by_days_since_read_a_2).alias("days_since_read_at_rank"),
        F.max("kwh").over(w_read_at_to_date_quality_by_r_4).alias("kwh_max_7"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at_week",
        "minutes_since_prev_read_at",
        "rows_in_read_at_month_quality",
        "days_since_read_at_seq",
        "days_since_read_at_vs_read_at_month_quality_avg",
        "days_since_read_at_share_of_read_at_week_year_qu",
        "days_since_read_at_percent_rank",
        "kwh_sum_1_prior",
        "kwh_change",
        "days_since_read_at_rank",
        "kwh_max_7",
        F.first("kwh_change").over(w_site_id_read_at_week_year_by_2).alias("first_kwh_change"),
        F.min("days_since_read_at_rank").over(w_site_id_quality).alias("site_id_quality_min_days_since_read_at_rank"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.row_number().over(w_days_since_read_at_rank_site).alias("kwh_sum_1_prior_seq"),
        F.round(F.cume_dist().over(w_days_since_read_at_rank_site_2), 3).alias("kwh_sum_1_prior_cume_dist"),
        F.round((F.unix_timestamp("read_at_week") - F.unix_timestamp(F.lag("read_at_week").over(w_site_id_read_at_week_year_by_3))) / 60, 1).alias("minutes_since_prev_read_at_week"),
    )
)

result = (
    sequenced
    .filter(F.col("site_id_quality_min_days_since_read_at_rank").between(31, 95))
    .select(
        "id",
        "site_id",
        "read_at_week",
        "minutes_since_prev_read_at",
        "days_since_read_at_seq",
        "days_since_read_at_vs_read_at_month_quality_avg",
        "days_since_read_at_share_of_read_at_week_year_qu",
        "days_since_read_at_percent_rank",
        "kwh_sum_1_prior",
        "kwh_change",
        "first_kwh_change",
        "site_id_quality_min_days_since_read_at_rank",
        "rows_in_site_id_quality",
        "kwh_sum_1_prior_seq",
        "minutes_since_prev_read_at_week",
        (F.col("kwh_sum_1_prior_seq") + F.col("rows_in_site_id_quality")).alias("kwh_sum_1_prior_seq_plus_rows_in_site_id_quality"),
        F.least(F.col("days_since_read_at_seq"), F.col("kwh_change")).alias("least_days_since_read_at_seq_kwh_change"),
    )
    .orderBy(
        "kwh_sum_1_prior_seq",
        "days_since_read_at_vs_read_at_month_quality_avg",
    )
)
