from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_kwh_full = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_kwh_prior = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(-1, -1)
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc_nulls_first(), "id")
w_site_id_quality_by_kwh_runni = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_2 = Window.orderBy(F.col("kwh").desc(), "id")
w_site_id_read_at_month = Window.partitionBy("site_id", F.date_trunc("month", "read_at"))
w_site_id_quality_by_kwh_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id")
w_site_id_quality_by_kwh_full_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_quality_by_kwh_prior_1 = Window.partitionBy("quality").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_kwh_quartile = Window.partitionBy("kwh_quartile")
w_quality_kwh_dense_rank_by_re = Window.partitionBy("quality", "kwh_dense_rank").orderBy("read_at", "id")
w_all_by_kwh_avg_1_prior = Window.orderBy(F.col("kwh_avg_1_prior").desc(), "id")
w_all_by_voltage_distance = Window.orderBy("voltage_distance", "id")
w_all_by_kwh_avg_1_prior_runni = Window.orderBy(F.col("kwh_avg_1_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_kwh_quartile_row_count_by_mi = Window.partitionBy("kwh_quartile", "row_count").orderBy("min_voltage", "kwh_quartile")
w_kwh_quartile_row_count_by_mi_2 = Window.partitionBy("kwh_quartile", "row_count").orderBy("min_voltage", "kwh_quartile").rowsBetween(-3, -1)
w_kwh_quartile_row_count = Window.partitionBy("kwh_quartile", "row_count")

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.first("kwh").over(w_site_id_quality_by_kwh_full).alias("first_kwh"),
        F.round(F.avg("kwh").over(w_site_id_quality_by_kwh_prior), 2).alias("kwh_avg_1_prior"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_quality_by_kwh)).alias("voltage_change"),
        F.ntile(4).over(w_all_by_kwh).alias("kwh_quartile"),
        F.max("voltage").over(w_site_id_quality_by_kwh_runni).alias("max_voltage_so_far"),
        F.ntile(5).over(w_all_by_kwh_2).alias("kwh_quintile"),
        F.count(F.lit(1)).over(w_site_id_read_at_month).alias("rows_in_site_id_read_at_month"),
        F.dense_rank().over(w_site_id_quality_by_kwh_2).alias("kwh_dense_rank"),
        F.first("read_at").over(w_site_id_quality_by_kwh_full_2).alias("first_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.max("kwh").over(w_quality_by_kwh_prior_1).alias("kwh_max_1_prior"),
    )
)

annotated = (
    sequenced
    .filter(F.col("kwh_avg_1_prior") >= 48.0)
    .select(
        "*",
        F.abs(F.col("voltage") - 220.7).alias("voltage_distance"),
        F.coalesce(F.col("max_voltage_so_far"), F.lit(0.0)).alias("max_voltage_so_far_filled"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.max("voltage_change").over(w_kwh_quartile).alias("kwh_quartile_max_voltage_change"),
        F.size(F.collect_set("quality").over(w_kwh_quartile)).alias("distinct_quality_in_kwh_quartile"),
        F.round(F.percent_rank().over(w_quality_kwh_dense_rank_by_re), 3).alias("read_at_percent_rank"),
        F.lead("kwh_avg_1_prior").over(w_all_by_kwh_avg_1_prior).alias("next_kwh_avg_1_prior"),
        F.rank().over(w_all_by_voltage_distance).alias("voltage_distance_rank"),
        F.round(F.col("max_voltage_so_far") / F.nullif(F.sum("max_voltage_so_far").over(w_kwh_quartile), F.lit(0)), 4).alias("max_voltage_so_far_share_of_kwh_quartile"),
        F.first("max_voltage_so_far", ignorenulls=True).over(w_all_by_kwh_avg_1_prior_runni).alias("first_known_max_voltage_so_far"),
    )
)

totals = (
    metrics
    .filter(F.col("kwh") <= 80.8)
    .groupBy("kwh_quartile")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("kwh_share_of_site_id").alias("total_kwh_share_of_site_id"),
        F.min("voltage").alias("min_voltage"),
        F.round(F.avg("max_voltage_so_far"), 2).alias("avg_max_voltage_so_far"),
        F.max("kwh_share_of_site_id").alias("max_kwh_share_of_site_id_check"),
    )
    .filter(F.col("max_kwh_share_of_site_id_check") > 0.1)
    .drop("max_kwh_share_of_site_id_check")
)

with_windows = (
    totals
    .select(
        "*",
        F.row_number().over(w_kwh_quartile_row_count_by_mi).alias("rn"),
        F.sum("avg_max_voltage_so_far").over(w_kwh_quartile_row_count_by_mi_2).alias("avg_max_voltage_so_far_sum_3_prior"),
        F.min("min_voltage").over(w_kwh_quartile_row_count).alias("kwh_quartile_row_count_min_min_voltage"),
    )
)

result = (
    with_windows
    .filter(F.col("kwh_quartile") < 9)
    .select(
        "kwh_quartile",
        "row_count",
        "total_kwh_share_of_site_id",
        "min_voltage",
        "avg_max_voltage_so_far",
        "rn",
        "avg_max_voltage_so_far_sum_3_prior",
        F.coalesce(F.col("min_voltage"), F.lit(0.0)).alias("min_voltage_filled"),
        (F.col("row_count") - F.col("rn")).alias("row_count_minus_rn"),
    )
    .orderBy("min_voltage_filled", F.col("rn").desc(), "kwh_quartile")
    .limit(10)
)
