from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_by_days_since_read_a = Window.partitionBy("quality").orderBy(F.col("days_since_read_at").desc())
w_site_id_by_days_since_read_a = Window.partitionBy("site_id").orderBy("days_since_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_meter_readings
    .filter((F.col("quality") != "EST") & (F.col("site_id") <= 7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)

ranked = (
    base
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "days_since_read_at",
        "greatest_kwh_voltage",
        "read_at_quarter",
        F.round(F.col("days_since_read_at") - F.avg("days_since_read_at").over(w_site_id), 2).alias("days_since_read_at_vs_site_id_avg"),
        F.round(F.cume_dist().over(w_quality_by_days_since_read_a), 3).alias("days_since_read_at_cume_dist"),
        F.nth_value("days_since_read_at", 2).over(w_site_id_by_days_since_read_a).alias("second_days_since_read_at"),
        F.max("voltage").over(w_site_id).alias("site_id_max_voltage"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
    )
)

filtered = (
    ranked
    .crossJoin(ranked.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

site_id_stats = (
    filtered
    .groupBy("site_id")
    .agg(F.max("kwh").alias("max_kwh"))
)

compared = (
    filtered.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

result = (
    compared
    .select(
        "id",
        F.col("max_kwh").alias("max_kwh_v"),
        F.col("days_since_read_at_cume_dist").alias("m_days_since_read_at_cume_dist"),
        F.col("kwh_minus_max").alias("kwh_minus_max_v"),
        F.col("kwh").alias("m_kwh"),
    )
    .unpivot(
        ids=["id"],
        values=["max_kwh_v", "m_days_since_read_at_cume_dist", "kwh_minus_max_v", "m_kwh"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
