from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

tagged = (
    matched
    .filter(F.col("site_id") <= 3)
    .select(
        "*",
        F.round(F.col("prior_voltage_1d") * 1.5, 2).alias("prior_voltage_1d_adj"),
        F.when(F.col("prior_voltage_1d") >= 980.0, "A").when(F.col("prior_voltage_1d") >= 490.0, "B").otherwise("C").alias("prior_voltage_1d_tier"),
    )
)

result = (
    tagged
    .groupBy("read_at", "prior_rows_1d")
    .pivot("prior_voltage_1d_tier", ["B", "C"])
    .agg(F.count("prior_voltage_1d_adj"))
    .orderBy("read_at", "prior_rows_1d")
)
