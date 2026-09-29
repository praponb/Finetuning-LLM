from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_commissioned_on_month_s = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "site_id")
w_site_id_quality_by_least_kwh = Window.partitionBy("site_id", "quality").orderBy("least_kwh_voltage", "id").rowsBetween(-6, -1)
w_site_tariff_read_at_to_date = Window.partitionBy("site_tariff", F.to_date("read_at")).orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at = Window.orderBy("read_at", "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_tariff_read_at_to_date_2 = Window.partitionBy("site_tariff", F.to_date("read_at")).orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, -1)

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "least_kwh_voltage",
        "quality_prefix",
        "site_tariff",
        "site_commissioned_on",
        F.size(F.collect_set("site_tariff").over(w_site_commissioned_on_month_s)).alias("distinct_site_tariff_in_site_commissioned_on_mon"),
        F.round(F.avg("least_kwh_voltage").over(w_site_id_quality_by_least_kwh), 2).alias("least_kwh_voltage_avg_6_prior"),
        F.last("read_at", ignorenulls=True).over(w_site_tariff_read_at_to_date).alias("last_known_read_at"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_read_at)).alias("voltage_change"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_site_commissioned_on)).alias("voltage_change_2"),
        F.size(F.collect_set("quality").over(w_site_commissioned_on_month_s)).alias("distinct_quality_in_site_commissioned_on_month_s"),
        F.round(F.avg("voltage").over(w_site_tariff_read_at_to_date_2), 2).alias("voltage_avg_3_prior"),
    )
)

labeled = (
    windowed
    .filter(F.col("read_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat_ws("-", F.col("quality_prefix"), F.col("quality")).alias("quality_prefix_quality_label"),
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("last_known_read_at"))) / 3600, 1).alias("hours_last_known_read_at_to_read_at"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "least_kwh_voltage",
        "quality_prefix",
        "site_commissioned_on",
        "least_kwh_voltage_avg_6_prior",
        "last_known_read_at",
        "voltage_change",
        "rows_in_site_id_quality",
        "distinct_quality_in_site_commissioned_on_month_s",
        "voltage_avg_3_prior",
        "quality_prefix_quality_label",
    )
    .orderBy(F.col("voltage_avg_3_prior").asc_nulls_last())
)
