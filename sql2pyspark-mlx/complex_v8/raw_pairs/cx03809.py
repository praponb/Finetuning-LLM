from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.voltage").alias("prior_voltage_12h"),
    )
)

screened = (
    matched
    .crossJoin(matched.agg(F.avg("prior_rows_12h").alias("overall_avg_prior_rows_12h")))
    .filter(F.col("prior_rows_12h") > F.col("overall_avg_prior_rows_12h"))
    .drop("overall_avg_prior_rows_12h")
)

site_id_stats = (
    screened
    .groupBy("site_id")
    .agg(F.percentile_approx("prior_rows_12h", 0.5).alias("median_prior_rows_12h"))
)

compared = (
    screened.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_rows_12h") >= F.col("g.median_prior_rows_12h"))
    .select(
        "s.*",
        "g.median_prior_rows_12h",
        F.round(F.col("s.prior_rows_12h") - F.col("g.median_prior_rows_12h"), 2).alias("prior_rows_12h_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        "read_at",
        "prior_rows_12h",
        "prior_voltage_12h",
        "prior_rows_12h_minus_median",
        (F.floor(F.col("prior_voltage_12h") / 250) * 250).alias("prior_voltage_12h_bucket"),
    )
    .orderBy(F.col("read_at").desc_nulls_first())
)
