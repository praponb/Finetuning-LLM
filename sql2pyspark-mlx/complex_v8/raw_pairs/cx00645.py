from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_site_tariff_by_volta = Window.partitionBy("site_id", "site_tariff").orderBy("voltage")
w_all_by_voltage_ties = Window.orderBy("voltage")
w_site_tariff_by_voltage = Window.partitionBy("site_tariff").orderBy(F.col("voltage").desc(), "id")
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_tariff_site_id_by_volta = Window.partitionBy("site_tariff", "site_id").orderBy("voltage", "id").rowsBetween(-2, Window.currentRow)
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 219.6)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

combined_details = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "TOU"), "inner")
    .filter(F.col("si.tariff").isin("TOU", "EV"))
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality_slug",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "site_id",
        "quality_slug",
        "site_tariff",
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        F.round(F.cume_dist().over(w_site_id_site_tariff_by_volta), 3).alias("voltage_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_voltage_ties), 3).alias("voltage_cume_dist_2"),
        F.row_number().over(w_site_tariff_by_voltage).alias("voltage_seq"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist_3"),
        F.lead("voltage", 2).over(w_site_tariff_by_voltage).alias("next2_voltage"),
        F.round(F.avg("voltage").over(w_site_tariff_site_id_by_volta), 2).alias("voltage_avg_3"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.sum("voltage").over(w_all_by_voltage_running).alias("running_voltage"),
    )
)

scored = (
    with_windows
    .filter(F.col("site_id_sum_voltage").between(770.0, 2002.0))
    .select(
        "*",
        (F.col("site_id_sum_voltage") + F.col("voltage_cume_dist_2")).alias("site_id_sum_voltage_plus_voltage_cume_dist_2"),
        F.when(F.col("rows_in_site_id") >= 15, "HIGH").when(F.col("rows_in_site_id") >= 8, "MEDIUM").otherwise("LOW").alias("rows_in_site_id_tier"),
        F.coalesce(F.col("site_tariff"), F.lit("UNKNOWN")).alias("site_tariff_or_unknown"),
    )
)

result = (
    scored
    .filter(F.col("site_id_avg_voltage") <= 234.5)
    .select(
        "id",
        "quality_slug",
        "site_tariff",
        "site_id_sum_voltage",
        "voltage_cume_dist",
        "voltage_cume_dist_2",
        "site_id_avg_voltage",
        "voltage_cume_dist_3",
        "next2_voltage",
        "voltage_avg_3",
        "rows_in_site_id",
        "site_id_sum_voltage_plus_voltage_cume_dist_2",
        "rows_in_site_id_tier",
        "site_tariff_or_unknown",
        F.coalesce(F.col("site_id_sum_voltage"), F.lit(0.0)).alias("site_id_sum_voltage_filled"),
        F.coalesce(F.col("next2_voltage"), F.lit(0.0)).alias("next2_voltage_filled"),
    )
    .orderBy(
        "quality_slug",
        F.col("site_id_sum_voltage_plus_voltage_cume_dist_2").desc(),
    )
)
