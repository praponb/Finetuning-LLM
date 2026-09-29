from pyspark.sql import functions as F

scoped = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("year", F.col("commissioned_on")).alias("commissioned_on_year"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.quality").isNotNull()), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    screened.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") >= 50.4), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        F.col("reading_count").alias("reading_count_v"),
        F.col("reading_count_2").alias("reading_count_2_v"),
    )
    .unpivot(
        ids=["id"],
        values=["reading_count_v", "reading_count_2_v"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
