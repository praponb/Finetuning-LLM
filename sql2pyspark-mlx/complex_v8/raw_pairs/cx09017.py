from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year = Window.partitionBy(F.year("read_at"))
w_quality_or_unknown_or_unknow = Window.partitionBy("quality_or_unknown_or_unknown")
w_all_by_voltage_prior_6 = Window.orderBy("voltage", "id").rowsBetween(-6, -1)

valid_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

filtered = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(215.8, 234.7)) & (F.col("m.quality") == "BAD"), "left_anti")
)

metrics = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_ref",
        "quality_or_unknown",
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_year), 2).alias("kwh_vs_read_at_year_avg"),
        F.round(F.avg("kwh").over(w_read_at_year), 2).alias("read_at_year_avg_kwh"),
    )
)

scored = (
    metrics
    .filter(F.col("read_at_year_avg_kwh") <= 66.0)
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_vs_read_at_year_avg",
        "read_at_year_avg_kwh",
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
        F.coalesce(F.col("quality_or_unknown"), F.lit("UNKNOWN")).alias("quality_or_unknown_or_unknown"),
    )
)

labeled = (
    scored
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.col("kwh_vs_read_at_year_avg"), F.lit(0)), 3).alias("kwh_per_kwh_vs_read_at_year_avg"),
        F.when(F.col("voltage") >= 233.1, "large").when(F.col("voltage") >= 223.3, "medium").otherwise("small").alias("voltage_tier"),
        F.round(F.col("kwh_vs_read_at_year_avg") * 1.5, 2).alias("kwh_vs_read_at_year_avg_adj"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_vs_read_at_year_avg",
        "read_at_plus_7d",
        "quality_or_unknown_or_unknown",
        "kwh_per_kwh_vs_read_at_year_avg",
        "kwh_vs_read_at_year_avg_adj",
        F.sum("kwh").over(w_quality_or_unknown_or_unknow).alias("quality_or_unknown_or_unknown_sum_kwh"),
        F.sum("kwh").over(w_all_by_voltage_prior_6).alias("kwh_sum_6_prior"),
    )
)

summary = (
    ranked
    .groupBy(
        F.to_date("read_at_plus_7d").alias("read_at_plus_7d_day"),
        "quality_or_unknown_or_unknown",
        F.year("read_at").alias("read_at_year"),
    )
    .agg(
        F.round(F.sum("quality_or_unknown_or_unknown_sum_kwh") / F.nullif(F.sum("kwh_per_kwh_vs_read_at_year_avg"), F.lit(0)), 3).alias("quality_or_unknown_or_unknown_sum_kwh_to_kwh_per"),
        F.max("read_at_plus_7d").alias("last_read_at_plus_7d"),
        F.countDistinct("quality_or_unknown_or_unknown").alias("distinct_quality_or_unknown_or_unknowns"),
    )
)

result = (
    summary
    .filter(F.col("quality_or_unknown_or_unknown") == "UNKNOWN")
    .select(
        "read_at_plus_7d_day",
        "quality_or_unknown_or_unknown",
        "read_at_year",
        "quality_or_unknown_or_unknown_sum_kwh_to_kwh_per",
        "last_read_at_plus_7d",
        "distinct_quality_or_unknown_or_unknowns",
    )
    .orderBy(
        "last_read_at_plus_7d",
        F.col("read_at_year").asc_nulls_last(),
        "read_at_plus_7d_day",
        "quality_or_unknown_or_unknown",
    )
    .limit(20)
)
