from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_count_by_read_at_run = Window.partitionBy("reading_count").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reading_count_by_read_at_pri = Window.partitionBy("reading_count").orderBy("read_at", "id").rowsBetween(-1, -1)
w_read_at_week_month_site_id = Window.partitionBy(F.month("read_at_week"), "site_id")
w_reading_count_by_read_at_ful = Window.partitionBy("reading_count").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

summarized_rows = (
    valid_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

metrics = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "reading_count",
        "reading_count_2",
        F.first("reading_count", ignorenulls=True).over(w_reading_count_by_read_at_run).alias("first_known_reading_count"),
        F.max("reading_count_2").over(w_reading_count_by_read_at_pri).alias("reading_count_2_max_1_prior"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_week_month_site_id), F.lit(0)), 4).alias("voltage_share_of_read_at_week_month_site_id"),
        F.last("last_reading_read_at").over(w_reading_count_by_read_at_ful).alias("last_last_reading_read_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "read_at",
        "reading_count",
        "reading_count_2",
        "first_known_reading_count",
        "reading_count_2_max_1_prior",
        "voltage_share_of_read_at_week_month_site_id",
        "last_last_reading_read_at",
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d"),
        F.abs(F.col("reading_count_2") - 4).alias("reading_count_2_distance"),
    )
    .orderBy(
        F.col("reading_count").desc(),
        F.col("voltage_share_of_read_at_week_month_site_id").asc(),
    )
)
