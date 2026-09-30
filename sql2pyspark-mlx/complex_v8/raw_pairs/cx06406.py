from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_moving_2 = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_tariff_by_read_at = Window.partitionBy("site_tariff").orderBy(F.col("read_at").desc_nulls_first(), "id")

scoped = (
    df_meter_readings
    .filter((F.col("site_id") <= 7) & (F.hour(F.col("read_at")).between(9, 12)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 233.2, "A").when(F.col("voltage") >= 218.2, "B").otherwise("C").alias("voltage_tier"),
    )
)

joined_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.voltage_tier",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

with_windows = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "voltage_tier",
        "site_tariff",
        "site_name",
        F.round(F.avg("kwh").over(w_quality_by_kwh_moving_2), 2).alias("kwh_avg_3"),
        F.round(F.cume_dist().over(w_all_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.size(F.collect_set("site_tariff").over(w_quality_site_id)).alias("distinct_site_tariff_in_quality_site_id"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_read_at)).alias("voltage_change"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_read_at)).alias("kwh_change"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.ntile(3).over(w_site_tariff_by_read_at).alias("read_at_tercile"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
    )
)

labeled = (
    with_windows
    .filter(F.upper(F.trim(F.col("site_name"))).like("R%"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage_tier",
        "site_tariff",
        "site_name",
        "read_at_cume_dist",
        "distinct_site_tariff_in_quality_site_id",
        "voltage_change",
        "kwh_change",
        "minutes_since_prev_read_at",
        "kwh_vs_quality_site_id_avg",
        "read_at_tercile",
        "read_at_percent_rank",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage_tier",
        "site_tariff",
        "read_at_cume_dist",
        "distinct_site_tariff_in_quality_site_id",
        "kwh_change",
        "minutes_since_prev_read_at",
        "kwh_vs_quality_site_id_avg",
        "read_at_tercile",
        "days_since_read_at",
        F.concat_ws(": ", F.col("voltage_tier"), F.col("site_name")).alias("voltage_tier_site_name_label"),
    )
    .orderBy("distinct_site_tariff_in_quality_site_id")
)
