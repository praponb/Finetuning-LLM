from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_quarter_to_date_site = Window.partitionBy(F.to_date("read_at_quarter"), "site_id").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_read_at_quarter_year_site_id = Window.partitionBy(F.year("read_at_quarter"), "site_id").orderBy("read_at", "id")
w_quality = Window.partitionBy("quality")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_rn_read_at_dense_rank = Window.partitionBy("rn", "read_at_dense_rank")
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), F.col("id").desc())

base = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 3)) & (F.col("kwh") <= 79.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.dense_rank().over(w_read_at_quarter_to_date_site).alias("read_at_dense_rank"),
        F.row_number().over(w_read_at_quarter_year_site_id).alias("rn"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.lead("voltage", 1, 0).over(w_read_at_quarter_year_site_id).alias("next_voltage"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_kwh)).alias("kwh_change"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.min("kwh").over(w_rn_read_at_dense_rank).alias("rn_read_at_dense_rank_min_kwh"),
        F.nth_value("read_at_dense_rank", 2).over(w_all_by_voltage_full).alias("second_read_at_dense_rank"),
        F.sum("rn").over(w_all_by_voltage_running).alias("running_rn"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_rn_read_at_dense_rank), 2).alias("kwh_vs_rn_read_at_dense_rank_avg"),
    )
)

latest_per_key = (
    with_trends
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count_2"),
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
    )
)

summarized_rows = (
    latest_per_key.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
        "m.avg_reading_voltage",
    )
)

labeled = (
    summarized_rows
    .select(
        "*",
        F.round(F.col("kwh_share_of_quality") * 1.07, 2).alias("kwh_share_of_quality_adj"),
    )
)

grouped = (
    labeled
    .groupBy("read_at_dense_rank")
    .agg(F.max("kwh").alias("max_kwh"), F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 3)
)

result = (
    grouped
    .select(
        "read_at_dense_rank",
        "max_kwh",
        "cnt",
        F.coalesce(F.col("read_at_dense_rank"), F.lit(0)).alias("read_at_dense_rank_filled"),
    )
)
