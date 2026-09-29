from pyspark.sql import functions as F

joined_ranges = (
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

derived = (
    joined_ranges
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-24").cast("date"))
    .select("*", F.date_trunc("day", F.col("read_at")).alias("read_at_day"))
)

screened = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 233.1) & (F.hour(F.col("m.read_at")).between(8, 11)), "left_anti")
)

result = (
    screened
    .filter(F.col("prior_voltage_3d") >= 428.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_3d",
        "prior_voltage_3d",
        "read_at_day",
    )
)
