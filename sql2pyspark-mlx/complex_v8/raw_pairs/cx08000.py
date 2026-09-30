from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality = Window.partitionBy("quality")
w_site_id_by_kwh_full_2 = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id_by_kwh_2 = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_kwh_tier = Window.partitionBy("kwh_tier")
w_all_by_read_at_ties = Window.orderBy("read_at")
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn = Window.partitionBy("rn")
w_quality_rn_by_voltage_vs_kwh = Window.partitionBy("quality", "rn").orderBy("voltage_vs_kwh_tier_avg", "id")
w_all_by_kwh_cume_dist_full = Window.orderBy(F.col("kwh_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_read_at_month_by_seco = Window.partitionBy(F.month("second_read_at")).orderBy(F.col("second_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no = Window.partitionBy("seq_no")
w_all_by_min_read_at_cume_dist = Window.orderBy("min_read_at_cume_dist", "read_at_month", "seq_no")

recent_readings = (
    df_meter_readings
    .filter(F.col("kwh").between(34.6, 118.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.when(F.col("kwh") >= 79.4, "A").when(F.col("kwh") >= 50.4, "B").otherwise("C").alias("kwh_tier"),
    )
)

windowed = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "quality_prefix",
        F.first("kwh").over(w_site_id_by_kwh_full).alias("first_kwh"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
        F.nth_value("read_at", 2).over(w_site_id_by_kwh_full_2).alias("second_read_at"),
        F.dense_rank().over(w_site_id_by_kwh).alias("kwh_dense_rank"),
        F.round(F.cume_dist().over(w_site_id_by_kwh_2), 3).alias("kwh_cume_dist"),
        F.ntile(5).over(w_site_id_by_kwh_2).alias("kwh_quintile"),
        F.count(F.lit(1)).over(w_kwh_tier).alias("rows_in_kwh_tier"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_kwh_tier), 2).alias("voltage_vs_kwh_tier_avg"),
        F.round(F.cume_dist().over(w_all_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.min("voltage").over(w_site_id_by_kwh_running).alias("min_voltage_so_far"),
        F.row_number().over(w_site_id_by_kwh).alias("rn"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.concat_ws("|", F.col("quality_prefix"), F.col("quality")).alias("quality_prefix_quality_label"),
        F.abs(F.col("kwh_vs_quality_avg") - -48.0).alias("kwh_vs_quality_avg_distance"),
    )
)

windowed_2 = (
    tagged
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "quality_prefix",
        "kwh_dense_rank",
        "kwh_cume_dist",
        "voltage_vs_kwh_tier_avg",
        "read_at_cume_dist",
        "rn",
        "quality_prefix_quality_label",
        "kwh_vs_quality_avg_distance",
        F.round(F.col("kwh_vs_quality_avg") / F.nullif(F.sum("kwh_vs_quality_avg").over(w_rn), F.lit(0)), 4).alias("kwh_vs_quality_avg_share_of_rn"),
        F.row_number().over(w_quality_rn_by_voltage_vs_kwh).alias("seq_no"),
        F.first("kwh").over(w_all_by_kwh_cume_dist_full).alias("first_kwh_2"),
        F.max("first_kwh").over(w_second_read_at_month_by_seco).alias("max_first_kwh_so_far"),
    )
)

totals = (
    windowed_2
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "seq_no")
    .agg(
        F.round(100 * F.count(F.when(F.col("quality") == "OK", 1)) / F.count("*"), 1).alias("pct_quality_ok"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("read_at_cume_dist").alias("min_read_at_cume_dist"),
        F.round(F.sum("kwh_cume_dist"), 2).alias("total_kwh_cume_dist"),
    )
    .filter(F.col("cnt") >= 2)
)

ranked = (
    totals
    .withColumn("distinct_ids_share_of_seq_no", F.round(F.col("distinct_ids") / F.nullif(F.sum("distinct_ids").over(w_seq_no), F.lit(0)), 4))
    .withColumn("pct_quality_ok_change", F.col("pct_quality_ok") - F.lag("pct_quality_ok").over(w_all_by_min_read_at_cume_dist))
)

result = (
    ranked
    .filter(F.col("distinct_ids") >= 2)
    .select(
        "read_at_month",
        "cnt",
        "distinct_ids",
        "min_read_at_cume_dist",
        "total_kwh_cume_dist",
        "distinct_ids_share_of_seq_no",
        "pct_quality_ok_change",
        (F.floor(F.col("pct_quality_ok") / 25) * 25).alias("pct_quality_ok_bucket"),
    )
)
