from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_quality_by_kwh = Window.partitionBy(F.month("read_at"), "quality").orderBy("kwh", "id")
w_site_zone = Window.partitionBy("site_zone")
w_quality_site_id_by_kwh_runni = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_site_tariff_by_read_at = Window.partitionBy("site_tariff").orderBy("read_at", "id")
w_site_tariff_by_kwh = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id")
w_site_zone_read_at_to_date_by = Window.partitionBy("site_zone", F.to_date("read_at")).orderBy(F.col("kwh").desc(), "id")
w_site_tariff_by_read_at_prior = Window.partitionBy("site_tariff").orderBy("read_at", "id").rowsBetween(-1, -1)

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

combined_details_site = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.upper(F.trim(F.col("si.site_name"))).like("D%")), "inner")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

sequenced = (
    combined_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "days_since_read_at",
        "quality_prefix",
        "site_tariff",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_month_quality_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("days_since_read_at") - F.avg("days_since_read_at").over(w_site_zone), 2).alias("days_since_read_at_vs_site_zone_avg"),
        F.last("read_at", ignorenulls=True).over(w_quality_site_id_by_kwh_runni).alias("last_known_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_zone), 2).alias("kwh_vs_site_zone_avg"),
        F.sum("days_since_read_at").over(w_quality).alias("quality_sum_days_since_read_at"),
        F.round(F.cume_dist().over(w_site_tariff_by_read_at), 3).alias("read_at_cume_dist"),
        F.lead("kwh").over(w_site_tariff_by_kwh).alias("next_kwh"),
        F.count(F.lit(1)).over(w_site_zone).alias("rows_in_site_zone"),
        F.round(F.cume_dist().over(w_site_zone_read_at_to_date_by), 3).alias("kwh_cume_dist"),
        F.max("days_since_read_at").over(w_site_tariff_by_read_at_prior).alias("days_since_read_at_max_1_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "days_since_read_at",
        "quality_prefix",
        "site_tariff",
        "days_since_read_at_vs_site_zone_avg",
        "last_known_read_at",
        "kwh_vs_site_zone_avg",
        "quality_sum_days_since_read_at",
        "read_at_cume_dist",
        "next_kwh",
        "rows_in_site_zone",
        "kwh_cume_dist",
        "days_since_read_at_max_1_prior",
        F.round((F.unix_timestamp(F.col("last_known_read_at")) - F.unix_timestamp(F.col("read_at"))) / 3600, 1).alias("hours_read_at_to_last_known_read_at"),
        F.concat(F.col("quality_prefix"), F.lit("#"), F.col("days_since_read_at").cast("string")).alias("quality_prefix_ref"),
    )
    .orderBy(
        F.col("kwh_vs_site_zone_avg").desc(),
        F.col("quality_prefix_ref").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
