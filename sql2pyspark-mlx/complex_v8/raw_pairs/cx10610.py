from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_commissioned_on_quar = Window.partitionBy("zone").orderBy(F.col("commissioned_on_quarter").asc_nulls_last(), "id")
w_tariff_by_commissioned_on = Window.partitionBy("tariff").orderBy("commissioned_on", "id")
w_tariff_by_commissioned_on_ru = Window.partitionBy("tariff").orderBy("commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_commissioned_on_quarter_tari = Window.partitionBy("commissioned_on_quarter", "tariff")
w_all_by_commissioned_on_ties = Window.orderBy(F.col("commissioned_on").desc_nulls_first())

valid = (
    df_sites
    .filter((F.col("zone") != "Z4") & (F.col("installed_kw") >= 93.3))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
        F.quarter(F.col("commissioned_on")).alias("commissioned_on_quarter"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        "site_name_ref",
        "commissioned_on_quarter",
        F.dense_rank().over(w_zone_by_commissioned_on_quar).alias("commissioned_on_quarter_dense_rank"),
        F.lead("installed_kw").over(w_tariff_by_commissioned_on).alias("next_installed_kw"),
        F.count(F.lit(1)).over(w_tariff_by_commissioned_on_ru).alias("running_count"),
        F.size(F.collect_set("zone").over(w_commissioned_on_quarter_tari)).alias("distinct_zone_in_commissioned_on_quarter_tariff"),
        F.rank().over(w_all_by_commissioned_on_ties).alias("commissioned_on_rank"),
        F.lead("commissioned_on_quarter").over(w_zone_by_commissioned_on_quar).alias("next_commissioned_on_quarter"),
    )
)

zone_stats = (
    with_windows
    .groupBy("zone")
    .agg(F.max("commissioned_on_rank").alias("max_commissioned_on_rank"))
)

vs_group = (
    with_windows.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.commissioned_on_rank") == F.col("g.max_commissioned_on_rank"))
    .select(
        "s.*",
        "g.max_commissioned_on_rank",
        F.round(F.col("s.commissioned_on_rank") - F.col("g.max_commissioned_on_rank"), 2).alias("commissioned_on_rank_minus_max"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

profiled = (
    vs_group.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    profiled
    .select(
        "id",
        "site_name",
        "installed_kw",
        "site_name_ref",
        "commissioned_on_quarter",
        "next_installed_kw",
        "running_count",
        "distinct_zone_in_commissioned_on_quarter_tariff",
        "commissioned_on_rank",
        "next_commissioned_on_quarter",
        "commissioned_on_rank_minus_max",
        "first_reading_read_at",
    )
    .orderBy(F.col("commissioned_on_rank").asc_nulls_last())
)
