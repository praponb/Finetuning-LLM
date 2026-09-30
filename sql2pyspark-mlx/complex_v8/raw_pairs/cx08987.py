from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_count = Window.partitionBy("reading_count")
w_site_id_reading_count_by_rea = Window.partitionBy("site_id", "reading_count").orderBy("read_at")
w_site_id_reading_count_by_rea_2 = Window.partitionBy("site_id", "reading_count").orderBy("read_at", "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_by_read_at_prior_3 = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(-3, -1)
w_site_id_by_voltage_prior_3 = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id", "reading_id").rowsBetween(-3, -1)
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())

valid = (
    df_meter_readings
    .filter((F.col("voltage") >= 224.8) & (F.col("quality").isin("BAD", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    valid.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

ranked = (
    profiled
    .select(
        "*",
        F.round(F.col("reading_count") / F.nullif(F.sum("reading_count").over(w_reading_count), F.lit(0)), 4).alias("reading_count_share_of_reading_count"),
        F.rank().over(w_site_id_reading_count_by_rea).alias("read_at_rank"),
        F.row_number().over(w_site_id_reading_count_by_rea_2).alias("rn"),
        F.row_number().over(w_quality_by_read_at).alias("rn_2"),
        F.sum("kwh").over(w_quality_by_read_at_prior_3).alias("kwh_sum_3_prior"),
    )
)

line_level_readings = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 96.8), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.max_reading_voltage",
        "s.reading_count_share_of_reading_count",
        "s.rn_2",
        "s.kwh_sum_3_prior",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

metrics = (
    line_level_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "max_reading_voltage",
        "reading_count_share_of_reading_count",
        "rn_2",
        "kwh_sum_3_prior",
        "reading_id",
        "reading_kwh",
        F.round(F.avg("kwh_sum_3_prior").over(w_site_id_by_voltage_prior_3), 2).alias("kwh_sum_3_prior_avg_3_prior"),
        F.round(F.cume_dist().over(w_site_id_by_voltage_ties), 3).alias("voltage_cume_dist"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("voltage").alias("max_reading_voltage_2"),
    )
)

with_stats = (
    metrics.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.max_reading_voltage_2",
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "max_reading_voltage",
        "reading_count_share_of_reading_count",
        "rn_2",
        "kwh_sum_3_prior",
        "reading_id",
        "kwh_sum_3_prior_avg_3_prior",
        "voltage_cume_dist",
        "reading_count",
    )
    .orderBy("kwh")
)
