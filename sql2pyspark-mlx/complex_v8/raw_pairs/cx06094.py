from pyspark.sql import functions as F

valid = df_meter_readings.filter(F.col("read_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp"))

quality_stats = (
    valid
    .groupBy("quality")
    .agg(F.max("voltage").alias("max_voltage"))
)

outliers = (
    valid.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

tagged = (
    outliers
    .withColumn("least_voltage_minus_max_max_voltage", F.least(F.col("voltage_minus_max"), F.col("max_voltage")))
)

combined_details = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") >= F.lit("2024-10-28").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.max_voltage",
        "s.voltage_minus_max",
        "s.least_voltage_minus_max_max_voltage",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "least_voltage_minus_max_max_voltage",
        "voltage",
        "max_voltage",
        "kwh",
    )
    .unpivot(
        ids=["id"],
        values=["least_voltage_minus_max_max_voltage", "voltage", "max_voltage", "kwh"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
