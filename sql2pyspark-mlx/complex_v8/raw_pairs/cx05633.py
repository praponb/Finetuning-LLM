from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_quality_len = Window.partitionBy(F.month("read_at")).orderBy("quality_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_len = Window.partitionBy("quality_len")
w_site_id_quality_len = Window.partitionBy("site_id", "quality_len")
w_quality_by_quality_len = Window.partitionBy("quality").orderBy("quality_len", "id")
w_quality_site_id_by_site_comm = Window.partitionBy("quality", "site_id").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_by_quality_len_2 = Window.partitionBy(F.month("read_at")).orderBy("quality_len")
w_read_at_month_by_quality_len_3 = Window.partitionBy(F.month("read_at")).orderBy("quality_len", "id").rowsBetween(-3, -1)
w_quality_site_id_by_site_comm_2 = Window.partitionBy("quality", "site_id").orderBy("site_commissioned_on", "id")
w_read_at_month_by_quality_len_4 = Window.partitionBy(F.month("read_at")).orderBy("quality_len", "id")
w_read_at_month_quality_len_by = Window.partitionBy(F.date_trunc("month", "read_at"), "quality_len").orderBy(F.col("read_at_date").desc(), "id")
w_first_known_quality_len_2 = Window.partitionBy("first_known_quality_len_2")
w_all_by_distinct_site_zone_in = Window.orderBy("distinct_site_zone_in_quality_len", "id")
w_site_commissioned_on_year_by = Window.partitionBy(F.year("site_commissioned_on")).orderBy("quality_len", "id")
w_quality_len_quality_len_seq = Window.partitionBy("quality_len", "quality_len_seq").orderBy("rows_in_site_id_quality_len", "id")
w_site_commissioned_on_year_by_2 = Window.partitionBy(F.year("site_commissioned_on")).orderBy("quality_len", "id").rowsBetween(-1, -1)

base = (
    df_meter_readings
    .filter(F.col("quality").isin("BAD", "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

with_parent_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") == "Z4")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_windows = (
    with_parent_site
    .select(
        "id",
        "kwh",
        "quality",
        "quality_len",
        "site_commissioned_on",
        F.last("read_at").over(w_read_at_month_by_quality_len).alias("last_read_at"),
        F.size(F.collect_set("site_zone").over(w_quality_len)).alias("distinct_site_zone_in_quality_len"),
        F.min("kwh").over(w_site_id_quality_len).alias("site_id_quality_len_min_kwh"),
        F.lead("read_at_date", 3).over(w_quality_by_quality_len).alias("next3_read_at_date"),
        F.first("quality_len", ignorenulls=True).over(w_quality_site_id_by_site_comm).alias("first_known_quality_len"),
        F.size(F.collect_set("site_zone").over(w_site_id_quality_len)).alias("distinct_site_zone_in_site_id_quality_len"),
        F.first("quality_len", ignorenulls=True).over(w_site_id_by_kwh_running).alias("first_known_quality_len_2"),
        F.round(F.cume_dist().over(w_read_at_month_by_quality_len_2), 3).alias("quality_len_cume_dist"),
        F.count(F.lit(1)).over(w_site_id_quality_len).alias("rows_in_site_id_quality_len"),
        F.round(F.avg("kwh").over(w_read_at_month_by_quality_len_3), 2).alias("kwh_avg_3_prior"),
        F.row_number().over(w_quality_by_quality_len).alias("quality_len_seq"),
        F.datediff(F.col("read_at_date"), F.lag("read_at_date").over(w_quality_site_id_by_site_comm_2)).alias("days_since_prev_read_at_date"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_month_by_quality_len_4))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("quality_len") / F.nullif(F.sum("quality_len").over(w_quality_len), F.lit(0)), 4).alias("quality_len_share_of_quality_len"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_len), F.lit(0)), 4).alias("kwh_share_of_quality_len"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_month_quality_len_by))) / 60, 1).alias("minutes_since_prev_read_at_2"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.size(F.collect_set("quality").over(w_first_known_quality_len_2)).alias("distinct_quality_in_first_known_quality_len_2"),
        F.round(F.col("first_known_quality_len") / F.nullif(F.sum("first_known_quality_len").over(w_first_known_quality_len_2), F.lit(0)), 4).alias("first_known_quality_len_share_of_first_known_qua"),
        F.dense_rank().over(w_all_by_distinct_site_zone_in).alias("distinct_site_zone_in_quality_len_dense_rank"),
        F.lead("rows_in_site_id_quality_len").over(w_all_by_distinct_site_zone_in).alias("next_rows_in_site_id_quality_len"),
        F.round(F.percent_rank().over(w_site_commissioned_on_year_by), 3).alias("quality_len_percent_rank"),
        F.round(F.col("quality_len_cume_dist") / F.nullif(F.sum("quality_len_cume_dist").over(w_first_known_quality_len_2), F.lit(0)), 4).alias("quality_len_cume_dist_share_of_first_known_quali"),
        F.lag("next3_read_at_date", 2).over(w_quality_len_quality_len_seq).alias("prev2_next3_read_at_date"),
        F.max("kwh_avg_3_prior").over(w_site_commissioned_on_year_by_2).alias("kwh_avg_3_prior_max_1_prior"),
    )
)

result = (
    ranked
    .select("next3_read_at_date")
    .distinct()
    .orderBy(F.col("next3_read_at_date").desc())
)
