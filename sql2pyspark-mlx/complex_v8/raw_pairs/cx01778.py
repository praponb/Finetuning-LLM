from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_rate_full = Window.partitionBy(F.month("read_at")).orderBy(F.col("rate").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_month = Window.partitionBy("site_id", F.month("read_at"))
w_read_at_month_site_id_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("kwh", "id")
w_dedup = Window.partitionBy("zone", "site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

in_window = (
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

with_trends = (
    in_window
    .select(
        "*",
        F.first("energy_cost").over(w_read_at_month_by_rate_full).alias("first_energy_cost"),
        F.size(F.collect_set("zone").over(w_site_id_read_at_month)).alias("distinct_zone_in_site_id_read_at_month"),
        F.ntile(5).over(w_read_at_month_site_id_by_kwh).alias("kwh_quintile"),
        F.round(F.col("rate") - F.avg("rate").over(w_site_id_read_at_month), 2).alias("rate_vs_site_id_read_at_month_avg"),
        F.round(F.cume_dist().over(w_read_at_month_site_id_by_kwh), 3).alias("kwh_cume_dist"),
    )
)

deduped = (
    with_trends
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    deduped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

result = (
    with_stats
    .cube("distinct_zone_in_site_id_read_at_month", "kwh_quintile")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("zone") == "Z3", 1)).alias("n_zone_z3"),
        F.round(F.sum("energy_cost") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("energy_cost_to_kwh_ratio"),
    )
    .orderBy(
        F.col("distinct_zone_in_site_id_read_at_month").asc_nulls_last(),
        F.col("kwh_quintile").asc_nulls_last(),
    )
)
