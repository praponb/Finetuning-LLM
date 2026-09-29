from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")

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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

ranked = (
    covered
    .select(
        "*",
        F.first("energy_cost", ignorenulls=True).over(w_quality_by_read_at_running).alias("first_known_energy_cost"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_site_id), F.lit(0)), 4).alias("rate_share_of_site_id"),
        F.size(F.collect_set("zone").over(w_site_id)).alias("distinct_zone_in_site_id"),
        F.min("rate").over(w_site_id).alias("site_id_min_rate"),
    )
)

without_matches = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "left_anti")
)

annotated = (
    without_matches
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.col("first_known_energy_cost"), F.lit(0)), 3).alias("kwh_per_first_known_energy_cost"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

result = (
    annotated
    .groupBy("read_at")
    .pivot("zone", ["Z3", "Z2"])
    .agg(F.max("rate_share_of_site_id"))
)
