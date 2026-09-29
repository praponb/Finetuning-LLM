from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.sum("kwh").alias("total_reading_kwh"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.total_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .cube("quality", "site_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("read_at").alias("first_read_at"),
        F.round(F.sum("reading_count") / F.nullif(F.sum("total_reading_kwh"), F.lit(0)), 3).alias("reading_count_to_total_reading_kwh_ratio"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
