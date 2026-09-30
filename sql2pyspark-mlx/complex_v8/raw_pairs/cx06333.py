from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_quality_by_k = Window.partitionBy(F.to_date("read_at"), "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_voltage_prior_3 = Window.orderBy("voltage", "id").rowsBetween(-3, -1)
w_site_id_read_at_month = Window.partitionBy("site_id", F.date_trunc("month", "read_at"))
w_all_by_kwh_running = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_quality_by_k_2 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy(F.col("kwh").desc(), "id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_read_at_to_date_quality_by_k_3 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(-2, -1)
w_second_read_at_year_quality = Window.partitionBy(F.year("second_read_at"), "quality").orderBy(F.col("voltage_share_of_site_id_read_at_month").desc(), "id").rowsBetween(-2, Window.currentRow)
w_second_read_at_year_quality_2 = Window.partitionBy(F.year("second_read_at"), "quality").orderBy(F.col("voltage_share_of_site_id_read_at_month").desc(), "id")
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id_read_at_month", "kwh_seq")
w_second_read_at_year_quality_3 = Window.partitionBy(F.year("second_read_at"), "quality").orderBy(F.col("voltage_share_of_site_id_read_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_readings = df_meter_readings.filter(F.col("quality").isNotNull())

combined_details_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z3"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "si.site_name",
    )
)

metrics = (
    combined_details_site
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "site_name",
        F.nth_value("voltage", 2).over(w_read_at_to_date_quality_by_k).alias("second_voltage"),
        F.lead("kwh").over(w_all_by_kwh).alias("next_kwh"),
        F.max("kwh").over(w_all_by_voltage_prior_3).alias("kwh_max_3_prior"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_read_at_month), 2).alias("kwh_vs_site_id_read_at_month_avg"),
        F.count(F.lit(1)).over(w_all_by_kwh_running).alias("running_count"),
        F.row_number().over(w_read_at_to_date_quality_by_k_2).alias("kwh_seq"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_voltage)).alias("kwh_change"),
        F.size(F.collect_set("quality").over(w_site_id_read_at_month)).alias("distinct_quality_in_site_id_read_at_month"),
        F.round(F.avg("kwh").over(w_read_at_to_date_quality_by_k_3), 2).alias("kwh_avg_2_prior"),
        F.round(F.cume_dist().over(w_read_at_to_date_quality_by_k_2), 3).alias("kwh_cume_dist"),
        F.nth_value("read_at", 2).over(w_read_at_to_date_quality_by_k).alias("second_read_at"),
        F.count(F.lit(1)).over(w_site_id_read_at_month).alias("rows_in_site_id_read_at_month"),
        F.rank().over(w_all_by_voltage).alias("voltage_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_read_at_month), F.lit(0)), 4).alias("voltage_share_of_site_id_read_at_month"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.avg("voltage_rank").over(w_second_read_at_year_quality), 2).alias("voltage_rank_avg_3"),
        F.lag("read_at").over(w_second_read_at_year_quality_2).alias("prev_read_at"),
        F.min("kwh_cume_dist").over(w_distinct_quality_in_site_id).alias("distinct_quality_in_site_id_read_at_month_kwh_se"),
        F.nth_value("kwh_seq", 2).over(w_second_read_at_year_quality_3).alias("second_kwh_seq"),
    )
)

result = (
    windowed
    .filter(F.month(F.col("prev_read_at")).isin(1, 2))
    .select(
        "id",
        "voltage",
        "quality",
        "site_name",
        "second_voltage",
        "next_kwh",
        "kwh_max_3_prior",
        "kwh_vs_site_id_read_at_month_avg",
        "running_count",
        "kwh_seq",
        "kwh_change",
        "distinct_quality_in_site_id_read_at_month",
        "second_read_at",
        "rows_in_site_id_read_at_month",
        "voltage_rank",
        "voltage_share_of_site_id_read_at_month",
        "voltage_rank_avg_3",
        "prev_read_at",
        "distinct_quality_in_site_id_read_at_month_kwh_se",
        "second_kwh_seq",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
    .orderBy(F.col("site_name").desc(), F.col("kwh_seq").asc(), "id")
    .limit(10)
)
