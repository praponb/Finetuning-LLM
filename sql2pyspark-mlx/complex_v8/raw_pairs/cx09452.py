from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 20.4) & (F.col("voltage") <= 233.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

filtered = (
    base_readings
    .crossJoin(base_readings.agg(F.max("kwh").alias("overall_max_kwh")))
    .filter(F.col("kwh") < F.col("overall_max_kwh"))
    .drop("overall_max_kwh")
)

rollup_base = (
    filtered
    .groupBy("quality", F.year("read_at").alias("read_at_year"))
    .agg(
        F.max("kwh").alias("max_kwh"),
        F.count(F.when(F.col("quality").isin("BAD", "EST"), 1)).alias("n_quality_in_bad_est"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

tagged = (
    rollup_base
    .select(
        "*",
        F.abs(F.col("n_quality_in_bad_est") - 4).alias("n_quality_in_bad_est_distance"),
        F.round(F.col("max_kwh") / F.nullif(F.col("n_quality_in_bad_est"), F.lit(0)), 3).alias("max_kwh_per_n_quality_in_bad_est"),
        F.round(F.col("max_kwh") * 0.9, 2).alias("max_kwh_adj"),
    )
)

result = (
    tagged
    .select(
        "quality",
        "read_at_year",
        "max_kwh",
        "max_kwh_adj",
        "max_kwh_per_n_quality_in_bad_est",
    )
    .unpivot(
        ids=["quality", "read_at_year"],
        values=["max_kwh", "max_kwh_adj", "max_kwh_per_n_quality_in_bad_est"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
