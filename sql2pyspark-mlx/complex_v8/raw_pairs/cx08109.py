from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year = Window.partitionBy(F.year("read_at"))
w_quality_by_read_at_day = Window.partitionBy("quality").orderBy("read_at_day", "id")
w_quality_site_zone_by_read_at = Window.partitionBy("quality", "site_zone").orderBy("read_at_day")
w_quality_site_zone_by_read_at_2 = Window.partitionBy("quality", "site_zone").orderBy("read_at_day", "id").rowsBetween(-3, Window.currentRow)
w_quality_or_unknown_site_tari = Window.partitionBy("quality_or_unknown", "site_tariff")
w_all_by_voltage_share_of_read = Window.orderBy("voltage_share_of_read_at_year", "id")

valid = (
    df_meter_readings
    .filter((F.col("voltage") < 232.4) & (F.hour(F.col("read_at")).between(7, 9)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_day",
        "s.quality_or_unknown",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

ranked = (
    combined_details
    .withColumn("voltage_share_of_read_at_year", F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_year), F.lit(0)), 4))
    .withColumn("read_at_day_percent_rank", F.round(F.percent_rank().over(w_quality_by_read_at_day), 3))
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.rank().over(w_quality_site_zone_by_read_at).alias("read_at_day_rank"),
        F.round(F.avg("kwh").over(w_quality_site_zone_by_read_at_2), 2).alias("kwh_avg_4"),
        F.min("kwh").over(w_quality_or_unknown_site_tari).alias("quality_or_unknown_site_tariff_min_kwh"),
        F.count(F.lit(1)).over(w_quality_or_unknown_site_tari).alias("rows_in_quality_or_unknown_site_tariff"),
        F.lead("voltage", 2).over(w_all_by_voltage_share_of_read).alias("next2_voltage"),
        F.lead("kwh").over(w_all_by_voltage_share_of_read).alias("next_kwh"),
    )
)

with_parent_site = (
    ranked_2.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_day",
        "s.quality_or_unknown",
        "s.site_zone",
        "s.voltage_share_of_read_at_year",
        "s.read_at_day_percent_rank",
        "s.read_at_day_rank",
        "s.quality_or_unknown_site_tariff_min_kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff_2"),
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
    )
)

labeled = (
    with_parent_site
    .select(
        "id",
        "voltage",
        "quality",
        "quality_or_unknown",
        "site_zone",
        "read_at_day_percent_rank",
        "read_at_day_rank",
        "site_installed_kw",
        "site_tariff_2",
        "site_commissioned_on_2",
        F.dayofweek(F.col("site_commissioned_on_2")).alias("site_commissioned_on_2_dayofweek"),
    )
)

result = (
    labeled
    .filter(F.col("read_at_day_percent_rank") >= 0.2)
    .select("site_commissioned_on_2_dayofweek")
    .distinct()
    .orderBy("site_commissioned_on_2_dayofweek")
)
