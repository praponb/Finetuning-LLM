from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").asc_nulls_last())
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_read_at_to_date_site_tariff = Window.partitionBy(F.to_date("read_at"), "site_tariff").orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_all_by_site_commissioned_on_2 = Window.orderBy("site_commissioned_on")
w_all_by_kwh = Window.orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_distinct_site_tariff = Window.orderBy("distinct_site_tariff_in_quality_site_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_distinct_site_ta = Window.partitionBy("site_tariff", "distinct_site_tariff_in_quality_site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_quality = Window.partitionBy("site_tariff", "quality")
w_all_by_distinct_site_tariff_2 = Window.orderBy("distinct_site_tariff_in_quality_site_id", "id")
w_site_tariff_distinct_site_ta_2 = Window.partitionBy("site_tariff", "distinct_site_tariff_in_quality_site_id").orderBy("voltage", "id")
w_site_commissioned_on_rank = Window.partitionBy("site_commissioned_on_rank")
w_all_by_total_running_rows_in = Window.orderBy("total_running_rows_in_site_id_site_tariff", "site_commissioned_on_rank", "last_known_site_commissioned_on_rank")
w_site_commissioned_on_rank_by = Window.partitionBy("site_commissioned_on_rank").orderBy(F.col("first_next3_site_commissioned_on").desc(), "site_commissioned_on_rank", "last_known_site_commissioned_on_rank")

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_details_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") < F.lit("2025-05-07").cast("date"))
    .select(
        "s.*",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    with_details_site
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id_site_tariff).alias("rows_in_site_id_site_tariff"),
        F.nth_value("site_commissioned_on", 2).over(w_all_by_site_commissioned_on).alias("second_site_commissioned_on"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.size(F.collect_set("site_tariff").over(w_quality_site_id)).alias("distinct_site_tariff_in_quality_site_id"),
        F.round(F.avg("kwh_bucket").over(w_read_at_to_date_site_tariff), 2).alias("kwh_bucket_avg_4"),
        F.rank().over(w_all_by_site_commissioned_on_2).alias("site_commissioned_on_rank"),
        F.round(F.col("kwh_bucket") - F.avg("kwh_bucket").over(w_site_id_site_tariff), 2).alias("kwh_bucket_vs_site_id_site_tariff_avg"),
        F.dense_rank().over(w_all_by_kwh).alias("kwh_dense_rank_2"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_kwh)).alias("voltage_change"),
    )
)

first_rows = (
    metrics
    .filter(F.col("kwh_dense_rank_2") <= 3)
    .drop("kwh_dense_rank_2")
)

metrics_2 = (
    first_rows
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "kwh_bucket",
        "site_name",
        "site_commissioned_on",
        "site_tariff",
        "rows_in_site_id_site_tariff",
        "second_site_commissioned_on",
        "kwh_dense_rank",
        "distinct_site_tariff_in_quality_site_id",
        "kwh_bucket_avg_4",
        "site_commissioned_on_rank",
        "kwh_bucket_vs_site_id_site_tariff_avg",
        "voltage_change",
        F.sum("site_commissioned_on_rank").over(w_site_id_site_tariff).alias("site_id_site_tariff_sum_site_commissioned_on_ran"),
        F.sum("rows_in_site_id_site_tariff").over(w_all_by_distinct_site_tariff).alias("running_rows_in_site_id_site_tariff"),
        F.last("site_commissioned_on_rank", ignorenulls=True).over(w_site_tariff_distinct_site_ta).alias("last_known_site_commissioned_on_rank"),
        F.sum("voltage").over(w_site_tariff_quality).alias("site_tariff_quality_sum_voltage"),
        F.ntile(5).over(w_all_by_distinct_site_tariff_2).alias("distinct_site_tariff_in_quality_site_id_quintile"),
        F.lead("site_commissioned_on", 3).over(w_site_tariff_distinct_site_ta_2).alias("next3_site_commissioned_on"),
    )
)

summary = (
    metrics_2
    .filter(F.col("rows_in_site_id_site_tariff") > 8)
    .groupBy("site_commissioned_on_rank", "last_known_site_commissioned_on_rank")
    .agg(
        F.sum("running_rows_in_site_id_site_tariff").alias("total_running_rows_in_site_id_site_tariff"),
        F.min("next3_site_commissioned_on").alias("first_next3_site_commissioned_on"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.round(F.col("site_commissioned_on_rank") / F.nullif(F.sum("site_commissioned_on_rank").over(w_site_commissioned_on_rank), F.lit(0)), 4).alias("site_commissioned_on_rank_share_of_site_commissi"),
        F.row_number().over(w_all_by_total_running_rows_in).alias("rn"),
        F.dense_rank().over(w_site_commissioned_on_rank_by).alias("first_next3_site_commissioned_on_dense_rank"),
        F.count(F.lit(1)).over(w_site_commissioned_on_rank).alias("rows_in_site_commissioned_on_rank"),
    )
)

result = (
    with_windows
    .select(
        "site_commissioned_on_rank",
        "last_known_site_commissioned_on_rank",
        "first_next3_site_commissioned_on",
        "site_commissioned_on_rank_share_of_site_commissi",
        "rn",
        "first_next3_site_commissioned_on_dense_rank",
        "rows_in_site_commissioned_on_rank",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
        F.abs(F.col("rn") - 7).alias("rn_distance"),
    )
    .orderBy(
        F.col("site_commissioned_on_rank_share_of_site_commissi").asc_nulls_last(),
    )
)
