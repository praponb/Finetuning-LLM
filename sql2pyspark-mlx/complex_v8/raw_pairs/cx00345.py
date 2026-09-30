from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone = Window.partitionBy("site_zone")
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

joined_site = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.lower(F.col("si.site_name")).like("%ep%"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

metrics = (
    joined_site
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_zone), F.lit(0)), 4).alias("voltage_share_of_site_zone"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_zone), 2).alias("voltage_vs_site_zone_avg"),
        F.max("voltage").over(w_all_by_voltage_running).alias("max_voltage_so_far"),
        F.sum("voltage").over(w_site_zone).alias("site_zone_sum_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

profiled = (
    metrics.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["site_zone", "site_id"], ["site_zone"], ["site_id"]],
        "site_zone", "site_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("site_zone") == "Z4", 1).otherwise(0)).alias("n_site_zone_z4"),
    )
    .orderBy(F.col("site_zone").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
