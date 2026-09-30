from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_voltage_rank_site_id = Window.partitionBy("voltage_rank", "site_id")
w_all_by_rows_in_site_id_ties = Window.orderBy("rows_in_site_id")
w_quality_site_id_by_site_id_a = Window.partitionBy("quality", "site_id").orderBy("site_id_avg_kwh")

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix_2"),
        F.when(F.col("kwh") >= 82.9, "A").when(F.col("kwh") >= 42.1, "B").otherwise("C").alias("kwh_tier"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_prefix",
        "kwh_tier",
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.rank().over(w_quality_by_voltage).alias("voltage_rank"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_voltage_rank_site_id), 2).alias("voltage_vs_voltage_rank_site_id_avg"),
        F.rank().over(w_all_by_rows_in_site_id_ties).alias("rows_in_site_id_rank"),
        F.count(F.lit(1)).over(w_voltage_rank_site_id).alias("rows_in_voltage_rank_site_id"),
        F.max("rows_in_site_id").over(w_voltage_rank_site_id).alias("voltage_rank_site_id_max_rows_in_site_id"),
        F.rank().over(w_quality_site_id_by_site_id_a).alias("site_id_avg_kwh_rank"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 104.8), "left_semi")
    .filter(F.col("site_id_avg_voltage") <= 232.8)
)

joined = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

joined_site = (
    joined.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_prefix",
        "s.rows_in_site_id",
        "s.voltage_rank",
        "s.voltage_vs_voltage_rank_site_id_avg",
        "s.rows_in_voltage_rank_site_id",
        "s.voltage_rank_site_id_max_rows_in_site_id",
        "s.site_id_avg_kwh_rank",
        "s.site_installed_kw",
        "s.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

with_parent = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2025-11-28").cast("date")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_prefix",
        "s.rows_in_site_id",
        "s.voltage_rank",
        "s.voltage_vs_voltage_rank_site_id_avg",
        "s.rows_in_voltage_rank_site_id",
        "s.voltage_rank_site_id_max_rows_in_site_id",
        "s.site_id_avg_kwh_rank",
        "s.site_installed_kw",
        "s.site_zone",
        F.col("si.site_name").alias("site_site_name"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "quality_prefix",
        "rows_in_site_id",
        "voltage_rank",
        "voltage_rank_site_id_max_rows_in_site_id",
        "site_id_avg_kwh_rank",
        "site_installed_kw",
        "site_site_name",
        F.when(F.col("site_zone").isin("Z3", "Z4"), "group_a").when(F.col("site_zone") == "Z2", "group_b").otherwise("other").alias("site_zone_group"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
    .orderBy(F.col("site_zone_group").desc(), "id")
    .limit(20)
)
