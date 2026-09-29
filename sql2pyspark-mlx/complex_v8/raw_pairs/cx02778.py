from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_moving_2 = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at_moving_6 = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").asc_nulls_last(), "id")
w_quality_by_site_commissioned = Window.partitionBy("quality").orderBy("site_commissioned_on")
w_read_at_to_date_quality_by_s = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("site_commissioned_on")
w_read_at_month_by_read_at_day = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("read_at_day").desc(), "id")
w_read_at_to_date_site_id = Window.partitionBy(F.to_date("read_at"), "site_id")
w_quality_by_site_commissioned_2 = Window.partitionBy("quality").orderBy("site_commissioned_on", "id")
w_read_at_month_by_read_at_day_2 = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("read_at_day").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_commissioned_on_year_si = Window.partitionBy(F.year("site_commissioned_on"), "site_id")
w_quality_site_commissioned_on = Window.partitionBy("quality", "site_commissioned_on_dense_rank_2").orderBy(F.col("kwh_sum_7_adj").desc(), "id")
w_quality_site_commissioned_on_2 = Window.partitionBy("quality", "site_commissioned_on_dense_rank_2").orderBy(F.col("kwh_sum_7_adj").desc())
w_quality_site_commissioned_on_3 = Window.partitionBy("quality", "site_commissioned_on_dense_rank_2").orderBy(F.col("kwh_sum_7_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_commissioned_on_dense_r = Window.partitionBy("site_commissioned_on_dense_rank_2", "site_commissioned_on_dense_rank").orderBy("rows_in_read_at_to_date_site_id")
w_quality_site_commissioned_on_4 = Window.partitionBy("quality", "site_commissioned_on_dense_rank_2").orderBy(F.col("kwh_sum_7_adj").desc(), "id").rowsBetween(-6, Window.currentRow)
w_site_commissioned_on_dense_r_2 = Window.partitionBy("site_commissioned_on_dense_rank_2", "site_commissioned_on_dense_rank").orderBy("rows_in_read_at_to_date_site_id", "id").rowsBetween(-2, -1)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

with_parent_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_day",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_trends = (
    with_parent_site
    .select(
        "*",
        F.max("kwh").over(w_all_by_read_at_moving_2).alias("kwh_max_3"),
        F.sum("kwh").over(w_all_by_read_at_moving_6).alias("kwh_sum_7"),
        F.round(F.cume_dist().over(w_all_by_read_at), 3).alias("read_at_cume_dist"),
        F.dense_rank().over(w_quality_by_site_commissioned).alias("site_commissioned_on_dense_rank"),
        F.dense_rank().over(w_read_at_to_date_quality_by_s).alias("site_commissioned_on_dense_rank_2"),
        F.ntile(4).over(w_read_at_month_by_read_at_day).alias("read_at_day_quartile"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_read_at_to_date_site_id), 2).alias("site_installed_kw_vs_read_at_to_date_site_id_avg"),
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_quality_by_site_commissioned_2)).alias("site_installed_kw_change"),
        F.rank().over(w_quality_by_site_commissioned_2).alias("site_commissioned_on_rank"),
        F.first("kwh", ignorenulls=True).over(w_read_at_month_by_read_at_day_2).alias("first_known_kwh"),
        F.count(F.lit(1)).over(w_read_at_to_date_site_id).alias("rows_in_read_at_to_date_site_id"),
        F.round(F.avg("kwh").over(w_site_commissioned_on_year_si), 2).alias("site_commissioned_on_year_site_id_avg_kwh"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "site_name",
        "site_installed_kw",
        "kwh_max_3",
        "kwh_sum_7",
        "read_at_cume_dist",
        "site_commissioned_on_dense_rank",
        "site_commissioned_on_dense_rank_2",
        "site_installed_kw_vs_read_at_to_date_site_id_avg",
        "site_installed_kw_change",
        "site_commissioned_on_rank",
        "first_known_kwh",
        "rows_in_read_at_to_date_site_id",
        "site_commissioned_on_year_site_id_avg_kwh",
        F.coalesce(F.col("first_known_kwh"), F.lit(0.0)).alias("first_known_kwh_filled"),
        F.round(F.col("kwh_sum_7") * 0.25, 2).alias("kwh_sum_7_adj"),
        F.to_date(F.col("read_at_day")).alias("read_at_day_date"),
    )
)

windowed = (
    tagged
    .select(
        "id",
        "site_id",
        "quality",
        "site_name",
        "site_installed_kw",
        "kwh_max_3",
        "kwh_sum_7",
        "read_at_cume_dist",
        "site_commissioned_on_dense_rank",
        "site_commissioned_on_dense_rank_2",
        "site_installed_kw_vs_read_at_to_date_site_id_avg",
        "site_commissioned_on_rank",
        "first_known_kwh",
        "rows_in_read_at_to_date_site_id",
        "site_commissioned_on_year_site_id_avg_kwh",
        "first_known_kwh_filled",
        "kwh_sum_7_adj",
        "read_at_day_date",
        F.row_number().over(w_quality_site_commissioned_on).alias("kwh_sum_7_adj_seq"),
        F.dense_rank().over(w_quality_site_commissioned_on_2).alias("kwh_sum_7_adj_dense_rank"),
        F.nth_value("read_at", 2).over(w_quality_site_commissioned_on_3).alias("second_read_at"),
        F.round(F.cume_dist().over(w_site_commissioned_on_dense_r), 3).alias("rows_in_read_at_to_date_site_id_cume_dist"),
        F.round(F.avg("site_commissioned_on_rank").over(w_quality_site_commissioned_on_4), 2).alias("site_commissioned_on_rank_avg_7"),
        F.round(F.avg("first_known_kwh_filled").over(w_site_commissioned_on_dense_r_2), 2).alias("first_known_kwh_filled_avg_2_prior"),
    )
)

totals = (
    windowed
    .groupBy(
        "site_commissioned_on_dense_rank_2",
        "site_commissioned_on_dense_rank",
        F.date_trunc("month", "second_read_at").alias("second_read_at_month"),
    )
    .agg(
        F.round(F.sum("first_known_kwh") / F.nullif(F.sum("first_known_kwh_filled_avg_2_prior"), F.lit(0)), 3).alias("first_known_kwh_to_first_known_kwh_filled_avg_2_"),
        F.max("first_known_kwh").alias("max_first_known_kwh"),
    )
)

result = (
    totals
    .select(
        "site_commissioned_on_dense_rank_2",
        "site_commissioned_on_dense_rank",
        "first_known_kwh_to_first_known_kwh_filled_avg_2_",
        F.date_trunc("month", F.col("second_read_at_month")).alias("second_read_at_month_month"),
        F.abs(F.col("max_first_known_kwh") - 84.0).alias("max_first_known_kwh_distance"),
    )
    .orderBy(
        F.col("second_read_at_month_month").asc_nulls_last(),
        F.col("max_first_known_kwh_distance").desc_nulls_first(),
    )
)
