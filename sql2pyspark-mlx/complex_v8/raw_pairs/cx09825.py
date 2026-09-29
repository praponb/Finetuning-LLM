from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_zone = Window.partitionBy(F.month("read_at"), "zone")
w_zone_site_id_by_voltage_ties = Window.partitionBy("zone", "site_id").orderBy("voltage")

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

with_trends = (
    covered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        F.round(F.col("energy_cost") / F.nullif(F.sum("energy_cost").over(w_read_at_month_zone), F.lit(0)), 4).alias("energy_cost_share_of_read_at_month_zone"),
        F.rank().over(w_zone_site_id_by_voltage_ties).alias("voltage_rank"),
        F.size(F.collect_set("site_id").over(w_read_at_month_zone)).alias("distinct_site_id_in_read_at_month_zone"),
        F.sum("kwh").over(w_read_at_month_zone).alias("read_at_month_zone_sum_kwh"),
        F.max("voltage").over(w_read_at_month_zone).alias("read_at_month_zone_max_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "EST", 1)).alias("reading_quality_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

profiled = (
    with_trends.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.first_reading_read_at",
    )
)

result = (
    profiled
    .select(
        "id",
        "site_id",
        "read_at",
        "rate",
        "energy_cost_share_of_read_at_month_zone",
        "read_at_month_zone_sum_kwh",
        "reading_quality_count",
        "first_reading_read_at",
        F.round((F.unix_timestamp(F.col("first_reading_read_at")) - F.unix_timestamp(F.col("read_at"))) / 3600, 1).alias("hours_read_at_to_first_reading_read_at"),
        F.coalesce(F.col("voltage_rank"), F.lit(0)).alias("voltage_rank_filled"),
    )
    .orderBy("read_at_month_zone_sum_kwh", F.col("id").asc())
)
