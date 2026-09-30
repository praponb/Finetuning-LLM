from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

without_matches = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2024-11-21").cast("date")) & (F.upper(F.trim(F.col("si.site_name"))).like("R%")), "left_anti")
)

screened = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("voltage_bucket").alias("overall_avg_voltage_bucket")))
    .filter(F.col("voltage_bucket") > F.col("overall_avg_voltage_bucket"))
    .drop("overall_avg_voltage_bucket")
)

tagged = (
    screened
    .select(
        "*",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.round(F.col("voltage_bucket") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_bucket_per_kwh"),
    )
)

result = (
    tagged
    .groupingSets(
        [["quality_group", "read_at_quarter", "quality"], ["quality_group"], ["read_at_quarter"]],
        "quality_group", "read_at_quarter", "quality",
    )
    .agg(
        F.count(F.when(F.col("quality").isin("BAD", "EST"), 1)).alias("n_quality_in_bad_est"),
    )
)
