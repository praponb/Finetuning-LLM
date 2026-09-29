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
        F.sum("b.kwh").alias("prior_kwh_3d"),
    )
)

screened = (
    in_window.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "OK"), "left_anti")
)

with_activity = (
    screened.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(213.2, 235.8)), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "read_at",
        "prior_rows_3d",
        "prior_kwh_3d",
    )
    .orderBy(F.col("read_at").asc_nulls_last(), "id")
    .limit(5)
)
