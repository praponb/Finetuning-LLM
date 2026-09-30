from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.voltage").alias("prior_voltage_3d"),
    )
)

stats = (
    in_window
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.max("prior_rows_3d").alias("max_prior_rows_3d_check"),
    )
    .filter(F.col("max_prior_rows_3d_check") > 2)
    .drop("max_prior_rows_3d_check")
)

result = (
    stats
    .select("site_id", F.to_date(F.col("last_read_at")).alias("last_read_at_date"))
    .orderBy(F.col("last_read_at_date").desc(), "site_id")
)
