from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_reading_read_at_year_fi = Window.partitionBy(F.year("last_reading_read_at"), F.year("first_reading_read_at")).orderBy(F.col("first_reading_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_reading_read_at_year_fi_2 = Window.partitionBy(F.year("last_reading_read_at"), F.year("first_reading_read_at")).orderBy(F.col("first_reading_read_at").desc(), "id")
w_prior_rows_12h_site_id = Window.partitionBy("prior_rows_12h", "site_id")

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.voltage").alias("prior_voltage_12h"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_counts = (
    covered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at", "m.first_reading_read_at")
)

ranked = (
    with_counts
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_12h",
        "prior_voltage_12h",
        "last_reading_read_at",
        "first_reading_read_at",
        F.max("prior_voltage_12h").over(w_last_reading_read_at_year_fi).alias("max_prior_voltage_12h_so_far"),
        F.round((F.unix_timestamp("first_reading_read_at") - F.unix_timestamp(F.lag("first_reading_read_at").over(w_last_reading_read_at_year_fi_2))) / 60, 1).alias("minutes_since_prev_first_reading_read_at"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_last_reading_read_at_year_fi_2))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.avg("prior_voltage_12h").over(w_prior_rows_12h_site_id), 2).alias("prior_rows_12h_site_id_avg_prior_voltage_12h"),
    )
)

site_id_stats = (
    ranked
    .groupBy("site_id")
    .agg(F.max("prior_voltage_12h").alias("max_prior_voltage_12h"))
)

outliers = (
    ranked.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_voltage_12h") == F.col("g.max_prior_voltage_12h"))
    .select(
        "s.*",
        "g.max_prior_voltage_12h",
        F.round(F.col("s.prior_voltage_12h") - F.col("g.max_prior_voltage_12h"), 2).alias("prior_voltage_12h_minus_max"),
    )
)

result = (
    outliers
    .select(
        "id",
        "site_id",
        "prior_rows_12h",
        "prior_voltage_12h",
        "last_reading_read_at",
        "first_reading_read_at",
        "max_prior_voltage_12h_so_far",
        "minutes_since_prev_read_at",
        "prior_rows_12h_site_id_avg_prior_voltage_12h",
        "max_prior_voltage_12h",
        "prior_voltage_12h_minus_max",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_reading_read_at")).alias("days_since_last_reading_read_at"),
    )
    .orderBy(F.col("prior_voltage_12h").desc())
)
