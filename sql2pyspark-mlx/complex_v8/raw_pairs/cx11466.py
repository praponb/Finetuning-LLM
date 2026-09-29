from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")) | (F.col("voltage") <= 233.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)

enriched = (
    recent
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "read_at_quarter",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["reading_count", "quality_group", "reading_count_2"], ["reading_count"], ["quality_group"]],
        "reading_count", "quality_group", "reading_count_2",
    )
    .agg(F.max("reading_count_2").alias("max_reading_count_2"))
    .orderBy(
        F.col("reading_count").asc_nulls_last(),
        F.col("quality_group").asc_nulls_last(),
        F.col("reading_count_2").asc_nulls_last(),
    )
)
