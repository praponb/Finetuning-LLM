from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_group_2 = Window.partitionBy("quality_group_2")
w_quality_group_2_quality_grou = Window.partitionBy("quality_group_2", "quality_group").orderBy("kwh_distance", "id")
w_prev_read_at_month_quality_g = Window.partitionBy(F.date_trunc("month", "prev_read_at"), "quality_group_2")

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group_2"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.abs(F.col("kwh") - 48.2).alias("kwh_distance"),
        F.concat(F.col("quality_group"), F.lit("#"), F.col("id").cast("string")).alias("quality_group_ref"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"))
)

with_counts = (
    scored.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_kwh")
)

with_parent = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.length(F.trim(F.col("si.site_name"))) >= 7), "inner")
    .filter(F.col("si.commissioned_on") < F.lit("2025-05-07").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_group",
        "s.quality_group_2",
        "s.kwh_distance",
        "s.avg_reading_kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

labeled = (
    with_parent
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality_group")), " ", "_")).alias("quality_group_slug"),
        F.substring(F.trim(F.col("quality_group")), 1, 2).alias("quality_group_prefix"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality_group",
        "quality_group_2",
        "kwh_distance",
        "avg_reading_kwh",
        "site_commissioned_on",
        "quality_group_prefix",
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_group_2), 2).alias("voltage_vs_quality_group_2_avg"),
        F.lag("read_at").over(w_quality_group_2_quality_grou).alias("prev_read_at"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.size(F.collect_set("quality_group").over(w_prev_read_at_month_quality_g)).alias("distinct_quality_group_in_prev_read_at_month_qua"),
        F.count(F.lit(1)).over(w_prev_read_at_month_quality_g).alias("rows_in_prev_read_at_month_quality_group_2"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_prev_read_at_month_quality_g), 2).alias("voltage_vs_prev_read_at_month_quality_group_2_av"),
    )
)

result = (
    ranked_2
    .groupingSets(
        [["site_id", "quality_group_2"], ["site_id"], ["quality_group_2"], []],
        "site_id", "quality_group_2",
    )
    .agg(
        F.percentile_approx("avg_reading_kwh", 0.5).alias("median_avg_reading_kwh"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality_group_2").asc_nulls_last(),
    )
)
