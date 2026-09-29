from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_moving_6 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_prior_2 = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(-2, -1)

recent_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(6, 11))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

ranked = (
    recent_readings
    .select(
        "*",
        F.sum("kwh").over(w_site_id_by_voltage_moving_6).alias("kwh_sum_7"),
        F.last("kwh_filled", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_kwh_filled"),
        F.max("days_since_read_at").over(w_site_id_by_voltage_prior_2).alias("days_since_read_at_max_2_prior"),
    )
)

without_matches = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-02-06 14:00:00").cast("timestamp")) & (F.col("m.quality").isin("EST", "BAD")), "left_anti")
)

joined_site = (
    without_matches.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

tagged = (
    joined_site
    .select(
        "id",
        "kwh_sum_7",
        "last_known_kwh_filled",
        "days_since_read_at_max_2_prior",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

enriched = (
    tagged
    .filter(F.col("last_known_kwh_filled").between(42.0, 96.0))
    .select(
        "id",
        "kwh_sum_7",
        "last_known_kwh_filled",
        "days_since_read_at_max_2_prior",
        "read_at_quarter",
        (F.floor(F.col("kwh_sum_7") / 50) * 50).alias("kwh_sum_7_bucket"),
        F.round(F.col("read_at_quarter") / F.nullif(F.col("last_known_kwh_filled"), F.lit(0)), 3).alias("read_at_quarter_per_last_known_kwh_filled"),
        F.round(F.col("last_known_kwh_filled") / F.nullif(F.col("read_at_quarter"), F.lit(0)), 3).alias("last_known_kwh_filled_per_read_at_quarter"),
    )
)

filtered = (
    enriched
    .crossJoin(enriched.agg(F.min("last_known_kwh_filled").alias("overall_min_last_known_kwh_filled")))
    .filter(F.col("last_known_kwh_filled") > F.col("overall_min_last_known_kwh_filled"))
    .drop("overall_min_last_known_kwh_filled")
)

result = (
    filtered
    .select(
        "id",
        "kwh_sum_7",
        "last_known_kwh_filled",
        "days_since_read_at_max_2_prior",
        "read_at_quarter",
        "kwh_sum_7_bucket",
        (F.floor(F.col("kwh_sum_7") / 50) * 50).alias("kwh_sum_7_bucket_2"),
    )
    .orderBy("kwh_sum_7_bucket_2")
)
