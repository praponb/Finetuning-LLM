from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_read_at_week_mon = Window.partitionBy("site_tariff", F.month("read_at_week")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_read_at_week_mon_2 = Window.partitionBy("site_tariff", F.month("read_at_week")).orderBy("kwh")
w_site_tariff = Window.partitionBy("site_tariff")
w_quality_site_tariff = Window.partitionBy("quality", "site_tariff")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")
w_site_tariff_read_at_week_mon_3 = Window.partitionBy("site_tariff", F.month("read_at_week")).orderBy("kwh", "id")

prepared_readings = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

joined = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-11-28").cast("date"))), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

ranked = (
    joined
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "read_at_week",
        "site_name",
        F.first("kwh", ignorenulls=True).over(w_site_tariff_read_at_week_mon).alias("first_known_kwh"),
        F.dense_rank().over(w_site_tariff_read_at_week_mon_2).alias("kwh_dense_rank"),
        F.size(F.collect_set("site_id").over(w_site_tariff)).alias("distinct_site_id_in_site_tariff"),
        F.size(F.collect_set("site_id").over(w_quality_site_tariff)).alias("distinct_site_id_in_quality_site_tariff"),
        F.max("voltage").over(w_site_tariff).alias("site_tariff_max_voltage"),
        F.count(F.lit(1)).over(w_quality_site_tariff).alias("rows_in_quality_site_tariff"),
        F.count(F.lit(1)).over(w_site_tariff).alias("rows_in_site_tariff"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_site_tariff), 2).alias("site_installed_kw_vs_site_tariff_avg"),
        F.ntile(10).over(w_site_id_by_voltage).alias("voltage_decile"),
        F.min("voltage").over(w_site_tariff).alias("site_tariff_min_voltage"),
        F.round(F.percent_rank().over(w_site_tariff_read_at_week_mon_2), 3).alias("kwh_percent_rank"),
        F.round(F.avg("voltage").over(w_quality_site_tariff), 2).alias("quality_site_tariff_avg_voltage"),
        F.lag("kwh", 1, 0).over(w_site_tariff_read_at_week_mon_3).alias("prev_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_tariff), 2).alias("kwh_vs_site_tariff_avg"),
    )
)

result = (
    ranked
    .filter(F.col("site_id") >= 8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "read_at_week",
        "first_known_kwh",
        "kwh_dense_rank",
        "distinct_site_id_in_site_tariff",
        "distinct_site_id_in_quality_site_tariff",
        "site_tariff_max_voltage",
        "rows_in_quality_site_tariff",
        "rows_in_site_tariff",
        "site_installed_kw_vs_site_tariff_avg",
        "voltage_decile",
        "site_tariff_min_voltage",
        "kwh_percent_rank",
        "quality_site_tariff_avg_voltage",
        "kwh_vs_site_tariff_avg",
        F.dayofweek(F.col("read_at_week")).alias("read_at_week_dayofweek"),
    )
    .orderBy("rows_in_site_tariff", F.col("distinct_site_id_in_site_tariff").desc())
)
