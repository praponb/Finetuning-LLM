from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 241.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

rollup_base = (
    prepared_readings
    .filter(F.col("voltage_filled") >= 213.5)
    .groupBy(
        "site_id",
        "quality_or_unknown",
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
        F.round(F.sum("voltage_filled"), 2).alias("total_voltage_filled"),
        F.min("read_at").alias("first_read_at"),
        F.max("read_at").alias("last_read_at"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("voltage_filled"), 2).alias("stddev_voltage_filled"),
    )
    .filter(F.col("n_rows") >= 2)
)

scored = (
    rollup_base
    .select(
        "site_id",
        "quality_or_unknown",
        "read_at_month",
        "stddev_voltage",
        "last_read_at",
        "n_rows",
        "stddev_voltage_filled",
        F.lower(F.regexp_replace(F.trim(F.col("quality_or_unknown")), " ", "_")).alias("quality_or_unknown_slug"),
        F.when(F.col("quality_or_unknown").isin("UNKNOWN", "OK"), "group_a").when(F.col("quality_or_unknown") == "EST", "group_b").otherwise("other").alias("quality_or_unknown_group"),
        F.coalesce(F.col("quality_or_unknown"), F.lit("UNKNOWN")).alias("quality_or_unknown_or_unknown"),
    )
)

result = (
    scored
    .select(
        "site_id",
        "quality_or_unknown",
        F.col("stddev_voltage").alias("m_stddev_voltage"),
        F.col("stddev_voltage_filled").alias("m_stddev_voltage_filled"),
    )
    .unpivot(
        ids=["site_id", "quality_or_unknown"],
        values=["m_stddev_voltage", "m_stddev_voltage_filled"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
