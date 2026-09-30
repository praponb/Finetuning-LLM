from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 236.7) & (F.col("site_id") >= 7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_plus_15m"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

derived = (
    filtered_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "read_at_plus_15m",
        "read_at_date",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.date_format(F.col("read_at_plus_15m"), "yyyy-MM").alias("read_at_plus_15m_label"),
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
    )
)

result = (
    derived
    .select("id", "kwh", "voltage_adj")
    .unpivot(
        ids=["id"],
        values=["kwh", "voltage_adj"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
