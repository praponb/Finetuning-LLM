from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

stats = (
    valid_readings
    .filter(F.col("kwh") >= 35.0)
    .groupBy(
        F.date_format("read_at_hour", "yyyy-MM").alias("read_at_hour_ym"),
        F.year("read_at").alias("read_at_year"),
    )
    .agg(
        F.min("voltage").alias("min_voltage"),
        F.round(F.stddev("kwh"), 2).alias("stddev_kwh"),
        F.count("*").alias("n_rows"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)

scored = (
    stats
    .filter(F.col("stddev_kwh") <= 108.0)
    .select(
        "*",
        (F.col("read_at_year") - F.col("min_voltage")).alias("read_at_year_minus_min_voltage"),
        F.round(F.col("stddev_kwh") * 0.9, 2).alias("stddev_kwh_adj"),
        F.coalesce(F.col("min_voltage"), F.lit(0.0)).alias("min_voltage_filled"),
    )
)

result = (
    scored
    .filter(F.col("n_rows") <= 9)
    .select(
        "read_at_hour_ym",
        "min_voltage",
        "stddev_kwh",
        "n_rows",
        "distinct_site_ids",
        "read_at_year_minus_min_voltage",
        "stddev_kwh_adj",
        "min_voltage_filled",
    )
    .orderBy(F.col("stddev_kwh").desc(), F.col("min_voltage_filled").desc())
)
