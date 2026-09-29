from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_minus_kwh = Window.partitionBy("quality").orderBy("voltage_minus_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_quality_by_voltage_minus_kwh_2 = Window.partitionBy("quality").orderBy("voltage_minus_kwh", "id")
w_all_by_last_reading_read_at = Window.orderBy("last_reading_read_at", "quality").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_reading_read_at_to_date = Window.partitionBy(F.to_date("last_reading_read_at"), "distinct_quality_or_unknowns").orderBy(F.col("distinct_reading_ids").desc(), "quality")
w_distinct_quality_or_unknowns = Window.partitionBy("distinct_quality_or_unknowns")
w_all_by_last_reading_read_at_2 = Window.orderBy("last_reading_read_at", "quality").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_meter_readings
    .filter((F.col("kwh").between(10.9, 109.0)) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") <= 386.6) & (F.col("si.commissioned_on") >= F.lit("2024-08-24").cast("date")), "left_anti")
)

with_windows = (
    with_activity
    .select(
        "*",
        F.last("kwh", ignorenulls=True).over(w_quality_by_voltage_minus_kwh).alias("last_known_kwh"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.rank().over(w_quality_by_voltage_minus_kwh_2).alias("voltage_minus_kwh_rank"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_by_voltage_minus_kwh_2)).alias("kwh_change"),
    )
)

detail_rows_readings = (
    with_windows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.quality_or_unknown",
        "s.voltage_minus_kwh",
        "s.last_known_kwh",
        "s.voltage_vs_site_id_avg",
        "s.kwh_change",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

per_group = (
    detail_rows_readings
    .groupBy("quality")
    .agg(
        F.countDistinct("quality_or_unknown").alias("distinct_quality_or_unknowns"),
        F.count(F.when(F.col("reading_quality").isNotNull(), 1)).alias("n_reading_quality_is_not_null"),
        F.max("reading_read_at").alias("last_reading_read_at"),
        F.round(F.avg("voltage_vs_site_id_avg"), 2).alias("avg_voltage_vs_site_id_avg"),
        F.count("*").alias("n_rows"),
        F.countDistinct("reading_id").alias("distinct_reading_ids"),
    )
)

sequenced = (
    per_group
    .select(
        "*",
        F.last("avg_voltage_vs_site_id_avg", ignorenulls=True).over(w_all_by_last_reading_read_at).alias("last_known_avg_voltage_vs_site_id_avg"),
        (F.col("distinct_reading_ids") - F.lag("distinct_reading_ids").over(w_last_reading_read_at_to_date)).alias("distinct_reading_ids_change"),
        F.round(F.col("distinct_quality_or_unknowns") / F.nullif(F.sum("distinct_quality_or_unknowns").over(w_distinct_quality_or_unknowns), F.lit(0)), 4).alias("distinct_quality_or_unknowns_share_of_distinct_q"),
        F.last("last_reading_read_at").over(w_all_by_last_reading_read_at_2).alias("last_last_reading_read_at"),
    )
)

result = (
    sequenced
    .filter(F.col("distinct_reading_ids") <= 7)
    .select(
        "quality",
        "distinct_quality_or_unknowns",
        "last_reading_read_at",
        "avg_voltage_vs_site_id_avg",
        "n_rows",
        "distinct_reading_ids",
        "distinct_reading_ids_change",
        "distinct_quality_or_unknowns_share_of_distinct_q",
    )
    .orderBy(
        F.col("last_reading_read_at").desc_nulls_first(),
        F.col("n_rows").asc_nulls_last(),
    )
)
