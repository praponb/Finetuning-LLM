from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_count_by_kwh = Window.partitionBy("reading_quality_count").orderBy(F.col("kwh").desc_nulls_first(), "id")
w_reading_quality_count_by_kwh_2 = Window.partitionBy("reading_quality_count").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_zone = Window.partitionBy("zone")

covered = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
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

without_matches = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("BAD", "EST")) & (F.col("m.voltage") < 238.3), "left_anti")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
        F.min("voltage").alias("min_reading_voltage"),
    )
)

summarized_rows = (
    without_matches.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.min_reading_voltage",
    )
)

windowed = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "zone",
        "rate",
        "energy_cost",
        "reading_quality_count",
        F.row_number().over(w_reading_quality_count_by_kwh).alias("rn"),
        F.sum("kwh").over(w_reading_quality_count_by_kwh_2).alias("kwh_sum_3_prior"),
        F.size(F.collect_set("site_id").over(w_zone)).alias("distinct_site_id_in_zone"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "zone",
        "rate",
        "energy_cost",
        "reading_quality_count",
        "rn",
        "kwh_sum_3_prior",
        F.concat(F.col("zone"), F.lit("#"), F.col("rn").cast("string")).alias("zone_ref"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
    .orderBy(F.col("zone").desc_nulls_first(), "read_at")
)
