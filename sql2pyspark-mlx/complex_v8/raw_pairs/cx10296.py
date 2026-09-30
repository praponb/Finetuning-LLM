from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_by_voltage = Window.partitionBy("site_zone").orderBy("voltage", "id")
w_site_commissioned_on_month_q = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "quality").orderBy("voltage", "id").rowsBetween(-3, -1)
w_site_zone_by_voltage_ties = Window.partitionBy("site_zone").orderBy("voltage")
w_quality = Window.partitionBy("quality")
w_site_commissioned_on_month_q_2 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "quality").orderBy("voltage")
w_site_zone_by_voltage_running = Window.partitionBy("site_zone").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_quality_by_min_voltage_so_fa = Window.partitionBy("quality").orderBy(F.col("min_voltage_so_far").desc(), "id").rowsBetween(-2, -1)
w_all_by_min_voltage_so_far_ti = Window.orderBy("min_voltage_so_far")
w_all_by_min_voltage_so_far = Window.orderBy("min_voltage_so_far", "id")

filtered_readings = (
    df_meter_readings
    .filter((F.col("quality") != "OK") & (F.col("kwh").between(24.5, 110.5)))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_details_site = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

windowed = (
    with_details_site
    .select(
        "*",
        F.round(F.cume_dist().over(w_site_zone_by_voltage), 3).alias("voltage_cume_dist"),
        F.round(F.avg("kwh").over(w_site_commissioned_on_month_q), 2).alias("kwh_avg_3_prior"),
        F.round(F.percent_rank().over(w_site_zone_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.percent_rank().over(w_site_commissioned_on_month_q_2), 3).alias("voltage_percent_rank_2"),
        F.min("voltage").over(w_site_zone_by_voltage_running).alias("min_voltage_so_far"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.when(F.col("site_zone").isin("Z2", "Z4"), "group_a").when(F.col("site_zone") == "Z3", "group_b").otherwise("other").alias("site_zone_group"),
        F.coalesce(F.col("voltage_percent_rank_2"), F.lit(0.0)).alias("voltage_percent_rank_2_filled"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "kwh",
        "site_zone",
        "site_commissioned_on",
        "site_name",
        "voltage_cume_dist",
        "kwh_avg_3_prior",
        "voltage_percent_rank",
        "kwh_vs_quality_avg",
        "rows_in_quality",
        "voltage_vs_quality_avg",
        "quality_clean",
        "voltage_percent_rank_2_filled",
        F.count(F.lit(1)).over(w_quality_site_zone).alias("rows_in_quality_site_zone"),
        F.max("voltage").over(w_quality_site_zone).alias("quality_site_zone_max_voltage"),
        F.max("rows_in_quality").over(w_quality_by_min_voltage_so_fa).alias("rows_in_quality_max_2_prior"),
        F.rank().over(w_all_by_min_voltage_so_far_ti).alias("min_voltage_so_far_rank"),
        F.lead("site_commissioned_on").over(w_all_by_min_voltage_so_far).alias("next_site_commissioned_on"),
        F.size(F.collect_set("site_id").over(w_quality_site_zone)).alias("distinct_site_id_in_quality_site_zone"),
    )
)

result = (
    ranked
    .select(
        "id",
        "kwh",
        "site_zone",
        "site_commissioned_on",
        "site_name",
        "voltage_cume_dist",
        "voltage_percent_rank",
        "kwh_vs_quality_avg",
        "rows_in_quality",
        "voltage_vs_quality_avg",
        "quality_clean",
        "voltage_percent_rank_2_filled",
        "quality_site_zone_max_voltage",
        "next_site_commissioned_on",
        "distinct_site_id_in_quality_site_zone",
        F.date_trunc("week", F.col("site_commissioned_on")).alias("site_commissioned_on_week"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
    )
    .orderBy(F.col("id").desc(), "kwh_vs_quality_avg")
    .limit(20)
)
