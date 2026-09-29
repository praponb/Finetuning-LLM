from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_by_read_at_prior_6 = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_read_at_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_moving_6 = Window.orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_quality = Window.partitionBy("quality")
w_read_at_quartile_by_total_re = Window.partitionBy("read_at_quartile").orderBy(F.col("total_read_at_quartile").desc(), "quality_min_kwh_tier", "read_at_quartile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_min_kwh_tier_read_at = Window.partitionBy("quality_min_kwh_tier", "read_at_quartile")

prepared = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 232.0, "HIGH").when(F.col("voltage") >= 220.2, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_tier",
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.avg("voltage").over(w_quality_by_read_at_prior_6), 2).alias("voltage_avg_6_prior"),
        F.last("read_at").over(w_site_id_by_read_at_full).alias("last_read_at"),
        F.count(F.lit(1)).over(w_site_id_by_read_at_running).alias("running_count"),
        F.max("kwh").over(w_all_by_voltage_moving_6).alias("kwh_max_7"),
        F.first("read_at", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_read_at"),
        F.ntile(4).over(w_quality_by_read_at).alias("read_at_quartile"),
        F.lead("kwh", 1, 0).over(w_quality_by_read_at).alias("next_kwh"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
    )
)

per_group = (
    windowed
    .groupBy(
        F.when(F.col("quality_min_kwh") >= 720.0, "A").when(F.col("quality_min_kwh") >= 300.0, "B").otherwise("C").alias("quality_min_kwh_tier"),
        "read_at_quartile",
    )
    .agg(
        F.sum("kwh_max_7").alias("total_kwh_max_7"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
        F.sum(F.when(F.col("quality") == "EST", F.col("running_count")).otherwise(0)).alias("running_count_quality_est"),
        F.sum("read_at_quartile").alias("total_read_at_quartile"),
        F.round(F.stddev("kwh"), 2).alias("stddev_kwh"),
    )
)

metrics = (
    per_group
    .select(
        "*",
        F.nth_value("stddev_kwh", 2).over(w_read_at_quartile_by_total_re).alias("second_stddev_kwh"),
        F.count(F.lit(1)).over(w_quality_min_kwh_tier_read_at).alias("rows_in_quality_min_kwh_tier_read_at_quartile"),
    )
)

result = (
    metrics
    .filter(F.col("total_kwh_max_7") > 540.0)
    .select(
        "quality_min_kwh_tier",
        "read_at_quartile",
        "total_kwh_max_7",
        "running_count_quality_est",
        "total_read_at_quartile",
        "stddev_kwh",
        "second_stddev_kwh",
        "rows_in_quality_min_kwh_tier_read_at_quartile",
        F.substring(F.trim(F.col("quality_min_kwh_tier")), 1, 4).alias("quality_min_kwh_tier_prefix"),
        F.when(F.col("quality_min_kwh_tier").isin("C", "B"), "group_a").when(F.col("quality_min_kwh_tier") == "A", "group_b").otherwise("other").alias("quality_min_kwh_tier_group"),
    )
)
