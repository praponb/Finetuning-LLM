from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_prior_rows = Window.partitionBy(F.month("read_at")).orderBy("prior_rows_3d", "id")
w_read_at_month_site_id = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id")
w_read_at_month_by_prior_rows_2 = Window.partitionBy(F.month("read_at")).orderBy("prior_rows_3d", "id").rowsBetween(-1, Window.currentRow)

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.voltage").alias("prior_voltage_3d"),
    )
)

tagged = (
    matched
    .select(
        "*",
        F.round(F.col("prior_voltage_3d") * 1.1, 2).alias("prior_voltage_3d_adj"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_3d",
        "prior_voltage_3d",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_month_by_prior_rows))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.max("prior_rows_3d").over(w_read_at_month_site_id).alias("read_at_month_site_id_max_prior_rows_3d"),
        F.count(F.lit(1)).over(w_read_at_month_site_id).alias("rows_in_read_at_month_site_id"),
        F.round(F.avg("prior_rows_3d").over(w_read_at_month_by_prior_rows_2), 2).alias("prior_rows_3d_avg_2"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_counts = (
    ranked.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
    )
)

result = (
    with_counts
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_3d",
        "minutes_since_prev_read_at",
        "read_at_month_site_id_max_prior_rows_3d",
        "rows_in_read_at_month_site_id",
        "prior_rows_3d_avg_2",
        "reading_count",
        "first_reading_read_at",
        F.date_trunc("year", F.col("first_reading_read_at")).alias("first_reading_read_at_year"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("first_reading_read_at")).alias("days_since_first_reading_read_at"),
    )
    .orderBy(
        F.col("read_at_month_site_id_max_prior_rows_3d").desc_nulls_first(),
        F.col("prior_rows_3d_avg_2").desc_nulls_first(),
    )
)
