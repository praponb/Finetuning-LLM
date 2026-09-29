from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_group_by_max_kwh_per = Window.partitionBy("quality_group").orderBy(F.col("max_kwh_per_voltage").desc(), "quality_group", "last_reading_read_at_month")
w_all_by_last_last_reading_rea = Window.orderBy(F.col("last_last_reading_read_at").desc(), "quality_group", "last_reading_read_at_month")

base = (
    df_meter_readings
    .filter(F.col("site_id") >= 2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

derived = (
    base
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.count(F.when(F.col("quality").isin("BAD", "OK"), 1)).alias("reading_quality_count"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

stats = (
    summarized_rows
    .filter(F.col("kwh") > 30.2)
    .groupBy(
        "quality_group",
        F.date_trunc("month", "last_reading_read_at").alias("last_reading_read_at_month"),
    )
    .agg(
        F.max("kwh_per_voltage").alias("max_kwh_per_voltage"),
        F.sum("reading_quality_count").alias("total_reading_quality_count"),
        F.count("*").alias("row_count"),
        F.countDistinct("quality_group").alias("distinct_quality_groups"),
        F.max("last_reading_read_at").alias("last_last_reading_read_at"),
    )
)

qualified = (
    stats
    .crossJoin(stats.agg(F.avg("distinct_quality_groups").alias("overall_avg_distinct_quality_groups")))
    .filter(F.col("distinct_quality_groups") > F.col("overall_avg_distinct_quality_groups"))
    .drop("overall_avg_distinct_quality_groups")
)

with_trends = (
    qualified
    .select(
        "*",
        F.round((F.unix_timestamp("last_reading_read_at_month") - F.unix_timestamp(F.lag("last_reading_read_at_month").over(w_quality_group_by_max_kwh_per))) / 60, 1).alias("minutes_since_prev_last_reading_read_at_month"),
        F.round(F.cume_dist().over(w_quality_group_by_max_kwh_per), 3).alias("max_kwh_per_voltage_cume_dist"),
        F.row_number().over(w_all_by_last_last_reading_rea).alias("rn"),
    )
)

derived_2 = (
    with_trends
    .select(
        "*",
        F.coalesce(F.col("quality_group"), F.lit("UNKNOWN")).alias("quality_group_or_unknown"),
        F.round(F.col("max_kwh_per_voltage") * 1.1, 2).alias("max_kwh_per_voltage_adj"),
    )
)

result = (
    derived_2
    .select(
        "minutes_since_prev_last_reading_read_at_month",
        "total_reading_quality_count",
        "max_kwh_per_voltage",
    )
    .distinct()
    .orderBy(
        F.col("minutes_since_prev_last_reading_read_at_month").asc_nulls_last(),
        F.col("max_kwh_per_voltage").asc_nulls_last(),
    )
)
