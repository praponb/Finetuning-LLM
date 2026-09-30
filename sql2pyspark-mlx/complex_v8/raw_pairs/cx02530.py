from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.voltage").alias("prior_voltage_7d"),
    )
)

screened = (
    in_window.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.zone").isin("Z4", "Z3")) & (F.length(F.trim(F.col("si.site_name"))) >= 5), "left_semi")
)

result = (
    screened
    .rollup("site_id", "prior_rows_7d")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("prior_rows_7d").alias("min_prior_rows_7d"),
        F.round(F.sum("prior_voltage_7d"), 2).alias("total_prior_voltage_7d"),
        F.round(F.sum("prior_voltage_7d") / F.nullif(F.sum("prior_rows_7d"), F.lit(0)), 3).alias("prior_voltage_7d_to_prior_rows_7d_ratio"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("prior_rows_7d").asc_nulls_last(),
    )
)
