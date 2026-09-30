from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("kwh") < 84.9)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    cleaned.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

qualified = (
    profiled
    .crossJoin(profiled.agg(F.max("total_reading_voltage").alias("overall_max_total_reading_voltage")))
    .filter(F.col("total_reading_voltage") < F.col("overall_max_total_reading_voltage"))
    .drop("overall_max_total_reading_voltage")
)

annotated = (
    qualified
    .filter(F.col("voltage") < 234.5)
    .select(
        "id",
        "voltage",
        "total_reading_voltage",
        "reading_count",
        F.least(F.col("total_reading_voltage"), F.col("kwh")).alias("least_total_reading_voltage_kwh"),
    )
)

result = (
    annotated
    .filter(F.col("least_total_reading_voltage_kwh") >= 220.5)
    .select(
        "id",
        "voltage",
        "total_reading_voltage",
        "reading_count",
        "least_total_reading_voltage_kwh",
        F.when(F.col("reading_count") >= 5, "large").when(F.col("reading_count") >= 3, "medium").otherwise("small").alias("reading_count_tier"),
    )
)
