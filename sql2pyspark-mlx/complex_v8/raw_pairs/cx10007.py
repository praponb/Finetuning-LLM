from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("kwh") <= 84.9) & (F.col("site_id") >= 4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
        F.abs(F.col("kwh") - 68.1).alias("kwh_distance"),
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

joined_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone").isin("Z2", "Z4")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_quarter",
        "s.kwh_distance",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_stats = (
    joined_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage", "m.first_reading_read_at")
)

totals = (
    with_stats
    .filter(F.hour(F.col("read_at_quarter")).between(9, 15))
    .groupBy(F.year("first_reading_read_at").alias("first_reading_read_at_year"))
    .agg(
        F.array_sort(F.collect_set("site_zone")).alias("site_zone_list"),
        F.sum(F.when(F.col("quality").isNotNull(), 1).otherwise(0)).alias("n_quality_is_not_null"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.sum(F.when(F.upper(F.trim(F.col("site_name"))).like("M%"), F.col("kwh_distance"))).alias("kwh_distance_upper_trim_site_name_like_m"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    totals
    .select(
        "first_reading_read_at_year",
        "n_quality_is_not_null",
        "distinct_qualitys",
    )
    .unpivot(
        ids=["first_reading_read_at_year"],
        values=["n_quality_is_not_null", "distinct_qualitys"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
