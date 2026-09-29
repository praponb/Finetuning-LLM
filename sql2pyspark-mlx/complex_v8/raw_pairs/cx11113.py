from pyspark.sql import functions as F

valid = df_meter_readings.filter(F.hour(F.col("read_at")).between(9, 14))

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.count(F.when(F.col("quality").isin("EST", "BAD"), 1)).alias("reading_quality_count_2"),
    )
)

with_stats = (
    valid.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["site_id", "reading_quality_count"], ["site_id"], []],
        "site_id", "reading_quality_count",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("read_at").alias("first_read_at"),
        F.min("reading_quality_count").alias("min_reading_quality_count"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("reading_quality_count").asc_nulls_last(),
    )
)
