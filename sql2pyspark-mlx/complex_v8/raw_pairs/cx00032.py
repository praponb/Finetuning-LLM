from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_all_by_read_at_moving_1 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_reading_count = Window.partitionBy("reading_count")

recent_readings = (
    df_meter_readings
    .filter(F.col("voltage") <= 236.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    recent_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

labeled = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_group",
        "reading_count",
        F.when(F.col("voltage") >= 236.7, "A").when(F.col("voltage") >= 220.2, "B").otherwise("C").alias("voltage_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("quality_group")), " ", "_")).alias("quality_group_slug"),
    )
)

with_trends = (
    labeled
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.sum("reading_count").over(w_all_by_read_at_moving_1).alias("reading_count_sum_2"),
        F.sum("reading_count").over(w_reading_count).alias("reading_count_sum_reading_count"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff").isin("EV", "FLAT")), "left_semi")
)

result = (
    without_matches
    .cube("site_id", "voltage_tier")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("voltage_tier").asc_nulls_last(),
    )
)
