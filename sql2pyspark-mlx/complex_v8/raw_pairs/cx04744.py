from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_kwh_7d_ties = Window.orderBy(F.col("prior_kwh_7d").desc())
w_all_by_prior_kwh_7d_running = Window.orderBy(F.col("prior_kwh_7d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reading_count_site_id = Window.partitionBy("reading_count", "site_id")
w_all_by_prior_kwh_7d_full = Window.orderBy(F.col("prior_kwh_7d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.kwh").alias("prior_kwh_7d"),
    )
)

annotated = (
    matched
    .filter(F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_kwh_7d",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    annotated.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

metrics = (
    profiled
    .select(
        "id",
        "site_id",
        "prior_kwh_7d",
        "read_at_date",
        "max_reading_voltage",
        "reading_quality_count",
        "reading_count",
        F.round(F.percent_rank().over(w_all_by_prior_kwh_7d_ties), 3).alias("prior_kwh_7d_percent_rank"),
        F.first("read_at_date", ignorenulls=True).over(w_all_by_prior_kwh_7d_running).alias("first_known_read_at_date"),
        F.count(F.lit(1)).over(w_reading_count_site_id).alias("rows_in_reading_count_site_id"),
        F.round(F.cume_dist().over(w_all_by_prior_kwh_7d_ties), 3).alias("prior_kwh_7d_cume_dist"),
        F.last("read_at").over(w_all_by_prior_kwh_7d_full).alias("last_read_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "prior_kwh_7d",
        "max_reading_voltage",
        "reading_count",
        "prior_kwh_7d_percent_rank",
        "first_known_read_at_date",
        "rows_in_reading_count_site_id",
        "prior_kwh_7d_cume_dist",
    )
)
