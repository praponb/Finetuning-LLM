from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_zone = Window.partitionBy("zone")
w_read_at_month_quality_by_ene = Window.partitionBy(F.month("read_at"), "quality").orderBy(F.col("energy_cost").desc(), "id")
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy(F.col("read_at").desc())
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_site_zone_site_id_by_read_at = Window.partitionBy("site_zone", "site_id").orderBy("read_at", "id")

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_trends = (
    joined_ranges
    .select(
        "*",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_quality_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("energy_cost") - F.avg("energy_cost").over(w_zone), 2).alias("energy_cost_vs_zone_avg"),
        F.lag("energy_cost", 3).over(w_read_at_month_quality_by_ene).alias("prev3_energy_cost"),
        F.round(F.percent_rank().over(w_quality_by_read_at_ties), 3).alias("read_at_percent_rank"),
    )
)

with_details_site = (
    with_trends.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.rate",
        "s.energy_cost",
        "s.minutes_since_prev_read_at",
        "s.energy_cost_vs_zone_avg",
        "s.prev3_energy_cost",
        "s.read_at_percent_rank",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    with_details_site
    .select(
        "id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "energy_cost_vs_zone_avg",
        "read_at_percent_rank",
        "site_installed_kw",
        "site_tariff",
        F.count(F.lit(1)).over(w_site_id_site_tariff).alias("rows_in_site_id_site_tariff"),
        F.lag("rate", 2, 0).over(w_site_zone_site_id_by_read_at).alias("prev2_rate"),
    )
)

result = (
    metrics
    .select(
        "id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "energy_cost_vs_zone_avg",
        "read_at_percent_rank",
        "site_installed_kw",
        "rows_in_site_id_site_tariff",
    )
    .orderBy("rate", F.col("read_at").desc_nulls_first())
)
