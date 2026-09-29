from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_commissioned_on_month_s = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "site_id")
w_quality_site_id_by_site_comm = Window.partitionBy("quality", "site_id").orderBy(F.col("site_commissioned_on").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_site_id_by_site_commissioned = Window.partitionBy("site_id").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone = Window.partitionBy("site_zone")
w_quality_site_id_by_site_comm_2 = Window.partitionBy("quality", "site_id").orderBy(F.col("site_commissioned_on").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc(), "id")
w_all_by_site_commissioned_on_2 = Window.orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_by_site_commission = Window.partitionBy("site_zone").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_site_comm_3 = Window.partitionBy("quality", "site_id").orderBy(F.col("site_commissioned_on").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_quality_site_id_by_site_comm_4 = Window.partitionBy("quality", "site_id").orderBy(F.col("site_commissioned_on").desc_nulls_first())

scoped = (
    df_meter_readings
    .filter((F.col("voltage") > 222.6) | (F.col("kwh") >= 48.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

enriched_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_windows = (
    enriched_site
    .select(
        "id",
        "site_id",
        "quality",
        "site_commissioned_on",
        F.max("voltage").over(w_quality_site_id).alias("quality_site_id_max_voltage"),
        F.max("voltage").over(w_site_commissioned_on_month_s).alias("site_commissioned_on_month_site_id_max_voltage"),
        F.sum("voltage").over(w_quality_site_id_by_site_comm).alias("voltage_sum_6_prior"),
        F.last("site_commissioned_on", ignorenulls=True).over(w_site_id_by_site_commissioned).alias("last_known_site_commissioned_on"),
        F.min("voltage").over(w_site_zone).alias("site_zone_min_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.sum("voltage").over(w_quality_site_id_by_site_comm_2).alias("voltage_sum_3_prior"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_commissioned_on_month_s), F.lit(0)), 4).alias("voltage_share_of_site_commissioned_on_month_site"),
        F.ntile(4).over(w_all_by_site_commissioned_on).alias("site_commissioned_on_quartile"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.last("site_commissioned_on").over(w_all_by_site_commissioned_on_2).alias("last_site_commissioned_on"),
        F.last("voltage", ignorenulls=True).over(w_site_zone_by_site_commission).alias("last_known_voltage"),
        F.round(F.avg("voltage").over(w_site_zone), 2).alias("site_zone_avg_voltage"),
        F.round(F.avg("voltage").over(w_quality_site_id_by_site_comm_3), 2).alias("voltage_avg_2_prior"),
        F.round(F.cume_dist().over(w_quality_site_id_by_site_comm_4), 3).alias("site_commissioned_on_cume_dist"),
    )
)

labeled = (
    with_windows
    .filter(F.col("voltage_sum_6_prior") > 420.0)
    .select(
        "*",
        (F.floor(F.col("site_zone_min_voltage") / 500) * 500).alias("site_zone_min_voltage_bucket"),
        F.when(F.col("site_zone_avg_voltage") >= 234.5, "A").when(F.col("site_zone_avg_voltage") >= 222.2, "B").otherwise("C").alias("site_zone_avg_voltage_tier"),
        F.trunc(F.col("site_commissioned_on"), "MM").alias("site_commissioned_on_month_start"),
    )
)

result = (
    labeled
    .select(
        "id",
        "quality",
        "site_commissioned_on",
        "quality_site_id_max_voltage",
        "voltage_sum_6_prior",
        "last_known_site_commissioned_on",
        "voltage_sum_3_prior",
        "voltage_share_of_site_commissioned_on_month_site",
        "site_commissioned_on_quartile",
        "last_site_commissioned_on",
        "last_known_voltage",
        "site_zone_avg_voltage",
        "site_commissioned_on_cume_dist",
        "site_zone_min_voltage_bucket",
        "site_zone_avg_voltage_tier",
        "site_commissioned_on_month_start",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
    .orderBy("last_site_commissioned_on")
)
