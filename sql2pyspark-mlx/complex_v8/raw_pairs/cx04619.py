from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_site_id = Window.partitionBy("site_tariff", "site_id")
w_site_id_site_tariff_by_site = Window.partitionBy("site_id", "site_tariff").orderBy("site_commissioned_on")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id_site_tariff_by_site_2 = Window.partitionBy("site_id", "site_tariff").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh_moving_6 = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_site_id_by_site_commissioned = Window.partitionBy("site_id").orderBy("site_commissioned_on")
w_site_id_site_tariff_by_site_3 = Window.partitionBy("site_id", "site_tariff").orderBy("site_commissioned_on", "id").rowsBetween(-1, -1)
w_site_id_by_site_commissioned_2 = Window.partitionBy("site_id").orderBy("site_commissioned_on", "id").rowsBetween(-6, -1)
w_site_commissioned_on_rank = Window.partitionBy("site_commissioned_on_rank")
w_site_commissioned_on_rank_by = Window.partitionBy("site_commissioned_on_rank").orderBy(F.col("prev_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_commissioned_on_rank_by_2 = Window.partitionBy("site_commissioned_on_rank").orderBy(F.col("prev_read_at").desc())

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

enriched = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "site_tariff",
        "site_name",
        F.max("kwh").over(w_site_tariff_site_id).alias("site_tariff_site_id_max_kwh"),
        F.rank().over(w_site_id_site_tariff_by_site).alias("site_commissioned_on_rank"),
        F.lag("read_at").over(w_site_id_by_kwh).alias("prev_read_at"),
        F.round(F.percent_rank().over(w_site_id_site_tariff_by_site), 3).alias("site_commissioned_on_percent_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_tariff_site_id), 2).alias("kwh_vs_site_tariff_site_id_avg"),
        F.sum("kwh").over(w_site_id_site_tariff_by_site_2).alias("running_kwh"),
        F.count(F.lit(1)).over(w_site_tariff_site_id).alias("rows_in_site_tariff_site_id"),
        F.sum("kwh").over(w_site_id_by_kwh_running).alias("running_kwh_2"),
        F.round(F.avg("kwh").over(w_site_id_by_kwh_moving_6), 2).alias("kwh_avg_7"),
        F.min("kwh").over(w_site_tariff_site_id).alias("site_tariff_site_id_min_kwh"),
        F.round(F.percent_rank().over(w_site_id_by_site_commissioned), 3).alias("site_commissioned_on_percent_rank_2"),
        F.round(F.avg("kwh").over(w_site_id_site_tariff_by_site_3), 2).alias("kwh_avg_1_prior"),
        F.sum("kwh").over(w_site_id_by_site_commissioned_2).alias("kwh_sum_6_prior"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "read_at",
        "kwh",
        "site_tariff",
        "site_name",
        "site_tariff_site_id_max_kwh",
        "site_commissioned_on_rank",
        "prev_read_at",
        "running_kwh",
        "rows_in_site_tariff_site_id",
        "running_kwh_2",
        "kwh_avg_7",
        "site_tariff_site_id_min_kwh",
        "site_commissioned_on_percent_rank_2",
        "kwh_sum_6_prior",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev_read_at")).alias("days_since_prev_read_at"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.when(F.col("site_tariff").isin("TOU", "FLAT"), "group_a").when(F.col("site_tariff") == "EV", "group_b").otherwise("other").alias("site_tariff_group"),
    )
)

with_trends_2 = (
    tagged
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_commissioned_on_rank).alias("rows_in_site_commissioned_on_rank"),
        F.nth_value("days_since_prev_read_at", 2).over(w_site_commissioned_on_rank_by).alias("second_days_since_prev_read_at"),
        F.sum("running_kwh").over(w_site_commissioned_on_rank).alias("site_commissioned_on_rank_sum_running_kwh"),
        F.round(F.col("running_kwh_2") / F.nullif(F.sum("running_kwh_2").over(w_site_commissioned_on_rank), F.lit(0)), 4).alias("running_kwh_2_share_of_site_commissioned_on_rank"),
        F.dense_rank().over(w_site_commissioned_on_rank_by_2).alias("prev_read_at_dense_rank"),
        F.min("kwh_sum_6_prior").over(w_site_commissioned_on_rank).alias("site_commissioned_on_rank_min_kwh_sum_6_prior"),
    )
)

per_group = (
    with_trends_2
    .filter(F.col("site_tariff_site_id_max_kwh") >= 420.0)
    .groupBy(F.to_date("prev_read_at").alias("prev_read_at_day"))
    .agg(
        F.round(F.sum("running_kwh"), 2).alias("total_running_kwh"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("site_tariff").alias("distinct_site_tariffs"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    per_group
    .filter(F.col("prev_read_at_day") < F.lit("2025-06-01").cast("date"))
    .select(
        "prev_read_at_day",
        "total_running_kwh",
        "n_rows",
        "distinct_ids",
        "distinct_site_tariffs",
        F.round(F.col("distinct_site_tariffs") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("distinct_site_tariffs_per_distinct_ids"),
        F.greatest(F.col("n_rows"), F.col("distinct_site_tariffs")).alias("greatest_n_rows_distinct_site_tariffs"),
    )
    .orderBy("n_rows")
)
