from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_read_at = Window.partitionBy("read_at_year").orderBy("read_at", "id")
w_site_id = Window.partitionBy("site_id")
w_read_at_year_by_read_at_ties = Window.partitionBy("read_at_year").orderBy("read_at")
w_site_tariff_site_id_by_kwh_m = Window.partitionBy("site_tariff", "site_id").orderBy("kwh", "id").rowsBetween(-3, Window.currentRow)
w_site_tariff_site_id_by_kwh_r = Window.partitionBy("site_tariff", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_moving_2 = Window.orderBy("read_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_site_id_by_kwh = Window.partitionBy("site_tariff", "site_id").orderBy("kwh", "id")
w_quality_site_tariff = Window.partitionBy("quality", "site_tariff")

filtered = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(8, 14)) | (F.col("quality").isin("OK", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isin("FLAT", "EV"))
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

metrics = (
    combined_details
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "read_at_day",
        "site_tariff",
        F.lead("voltage").over(w_read_at_year_by_read_at).alias("next_voltage"),
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.round(F.cume_dist().over(w_read_at_year_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.avg("read_at_year").over(w_site_tariff_site_id_by_kwh_m), 2).alias("read_at_year_avg_4"),
        F.first("read_at_year", ignorenulls=True).over(w_site_tariff_site_id_by_kwh_r).alias("first_known_read_at_year"),
        F.round(F.avg("kwh").over(w_all_by_read_at_moving_2), 2).alias("kwh_avg_3"),
        F.last("voltage", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_voltage"),
        F.lead("read_at_day", 3).over(w_site_tariff_site_id_by_kwh).alias("next3_read_at_day"),
        F.round(F.col("read_at_year") - F.avg("read_at_year").over(w_site_id), 2).alias("read_at_year_vs_site_id_avg"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "read_at_day",
        "next_voltage",
        "site_id_sum_kwh",
        "read_at_cume_dist",
        "kwh_avg_3",
        "last_known_voltage",
        "next3_read_at_day",
        F.round(F.col("site_id_sum_kwh") / F.nullif(F.sum("site_id_sum_kwh").over(w_quality_site_tariff), F.lit(0)), 4).alias("site_id_sum_kwh_share_of_quality_site_tariff"),
        F.count(F.lit(1)).over(w_quality_site_tariff).alias("rows_in_quality_site_tariff"),
        F.sum("last_known_voltage").over(w_quality_site_tariff).alias("quality_site_tariff_sum_last_known_voltage"),
    )
)

result = (
    ranked
    .select(
        "id",
        "read_at",
        "kwh",
        "next_voltage",
        "read_at_cume_dist",
        "kwh_avg_3",
        "site_id_sum_kwh_share_of_quality_site_tariff",
        "quality_site_tariff_sum_last_known_voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.date_trunc("quarter", F.col("read_at_day")).alias("read_at_day_quarter"),
    )
    .orderBy("next_voltage", F.col("kwh").desc())
)
