from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality_group_by_voltage_ful = Window.partitionBy("quality_group").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_group_by_voltage_run = Window.partitionBy("quality_group").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_group = Window.partitionBy("site_id", "quality_group")

cleaned = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) | (F.col("voltage") > 221.4))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.upper(F.trim(F.col("quality"))).alias("quality_clean_2"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

annotated = (
    cleaned
    .select(
        "*",
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_quality), 2).alias("voltage_vs_site_id_quality_avg"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_quality), F.lit(0)), 4).alias("kwh_share_of_site_id_quality"),
        F.nth_value("voltage", 2).over(w_quality_group_by_voltage_ful).alias("second_voltage"),
        F.count(F.lit(1)).over(w_quality_group_by_voltage_run).alias("running_count"),
        F.first("kwh_filled", ignorenulls=True).over(w_quality_group_by_voltage_run).alias("first_known_kwh_filled"),
        F.sum("kwh_filled").over(w_site_id_quality).alias("site_id_quality_sum_kwh_filled"),
    )
)

with_trends = (
    ranked
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "quality_clean",
        "quality_clean_2",
        "quality_prefix",
        "kwh_filled",
        "voltage_vs_site_id_quality_avg",
        "second_voltage",
        "running_count",
        "first_known_kwh_filled",
        "site_id_quality_sum_kwh_filled",
        F.max("second_voltage").over(w_site_id_quality_group).alias("site_id_quality_group_max_second_voltage"),
        F.round(F.col("second_voltage") / F.nullif(F.sum("second_voltage").over(w_site_id_quality_group), F.lit(0)), 4).alias("second_voltage_share_of_site_id_quality_group"),
    )
)

result = (
    with_trends
    .rollup("quality", "site_id")
    .agg(
        F.sum(F.when(F.col("quality").isin("OK", "EST"), 1).otherwise(0)).alias("n_quality_in_ok_est"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
