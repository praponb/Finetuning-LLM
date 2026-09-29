from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_site_commissioned_on_mo = Window.partitionBy(F.month("last_site_commissioned_on"), "site_commissioned_on_year").orderBy("site_commissioned_on_span_days")
w_last_site_commissioned_on_mo_2 = Window.partitionBy(F.month("last_site_commissioned_on"), "site_commissioned_on_year").orderBy("site_commissioned_on_span_days", "site_commissioned_on_year", "quality_or_unknown")

scoped_readings = (
    df_meter_readings
    .filter((F.col("voltage") >= 223.0) & (F.col("kwh") < 90.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_parent = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality_or_unknown",
        "s.voltage_plus_kwh",
        "s.quality_slug",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

derived = (
    with_parent
    .select(
        "id",
        "kwh",
        "quality_or_unknown",
        "quality_slug",
        "site_installed_kw",
        "site_commissioned_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on_2"),
    )
)

summary = (
    derived
    .filter(F.col("quality_slug").isNotNull())
    .groupBy(
        F.year("site_commissioned_on").alias("site_commissioned_on_year"),
        "quality_or_unknown",
    )
    .agg(
        F.round(F.sum("kwh") / F.nullif(F.sum("site_installed_kw"), F.lit(0)), 3).alias("kwh_to_site_installed_kw_ratio"),
        F.max("site_commissioned_on").alias("last_site_commissioned_on"),
        F.sum("days_since_site_commissioned_on_2").alias("total_days_since_site_commissioned_on_2"),
        F.round(F.sum("site_installed_kw"), 2).alias("total_site_installed_kw"),
        F.datediff(F.max("site_commissioned_on"), F.min("site_commissioned_on")).alias("site_commissioned_on_span_days"),
        F.round(F.avg("days_since_site_commissioned_on"), 2).alias("avg_days_since_site_commissioned_on"),
    )
)

enriched = (
    summary
    .filter(F.col("site_commissioned_on_span_days") <= 560)
    .withColumn("total_site_installed_kw_adj", F.round(F.col("total_site_installed_kw") * 1.07, 2))
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.cume_dist().over(w_last_site_commissioned_on_mo), 3).alias("site_commissioned_on_span_days_cume_dist"),
        F.rank().over(w_last_site_commissioned_on_mo_2).alias("site_commissioned_on_span_days_rank"),
        F.dense_rank().over(w_last_site_commissioned_on_mo_2).alias("site_commissioned_on_span_days_dense_rank"),
        F.round(F.percent_rank().over(w_last_site_commissioned_on_mo), 3).alias("site_commissioned_on_span_days_percent_rank"),
    )
)

result = (
    sequenced
    .groupBy("site_commissioned_on_year", "site_commissioned_on_span_days_dense_rank")
    .pivot("quality_or_unknown", ["UNKNOWN", "EST", "OK"])
    .agg(F.min("total_site_installed_kw_adj"))
    .orderBy(
        "site_commissioned_on_year",
        "site_commissioned_on_span_days_dense_rank",
    )
)
