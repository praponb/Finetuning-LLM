from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_site_zone = Window.partitionBy("zone", "site_zone")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone_read_at_year = Window.partitionBy("zone", F.year("read_at"))
w_site_id_by_rate_ties = Window.partitionBy("site_id").orderBy(F.col("rate").desc_nulls_first())

covered = (
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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_parent = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.zone",
        "s.rate",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "rate",
        "site_commissioned_on",
        F.min("kwh").over(w_zone_site_zone).alias("zone_site_zone_min_kwh"),
        F.count(F.lit(1)).over(w_all_by_read_at_running).alias("running_count"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_zone_read_at_year)).alias("distinct_site_id_in_zone_read_at_year"),
        F.round(F.col("running_count") - F.avg("running_count").over(w_zone_read_at_year), 2).alias("running_count_vs_zone_read_at_year_avg"),
        F.round(F.cume_dist().over(w_site_id_by_rate_ties), 3).alias("rate_cume_dist"),
    )
)

result = (
    ranked
    .filter(F.col("rate_cume_dist") <= 0.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "site_commissioned_on",
        "zone_site_zone_min_kwh",
        "running_count_vs_zone_read_at_year_avg",
        "rate_cume_dist",
        (F.col("zone_site_zone_min_kwh") - F.col("rate")).alias("zone_site_zone_min_kwh_minus_rate"),
        F.least(F.col("rate"), F.col("zone_site_zone_min_kwh")).alias("least_rate_zone_site_zone_min_kwh"),
    )
)
