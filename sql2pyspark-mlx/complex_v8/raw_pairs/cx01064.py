from pyspark.sql import functions as F

covered = (
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

derived = (
    covered
    .select(
        "*",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

result = (
    derived
    .filter(F.col("read_at_dayofweek").between(2, 5))
    .select(
        "id",
        "site_id",
        "prior_voltage_12h",
        "read_at_dayofweek",
        "read_at_label",
    )
    .orderBy(F.col("site_id").desc_nulls_first(), "id")
    .limit(10)
)
