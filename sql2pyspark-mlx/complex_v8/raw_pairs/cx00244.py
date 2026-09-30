from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.when(F.col("kwh") >= 106.5, "A").when(F.col("kwh") >= 33.9, "B").otherwise("C").alias("kwh_tier"),
    )
)

screened = (
    prepared_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-03-08").cast("date")) & (F.col("m.kwh") <= 109.0), "left_anti")
)

result = (
    screened
    .groupingSets(
        [["kwh_tier", "quality"], ["kwh_tier"]],
        "kwh_tier", "quality",
    )
    .agg(
        F.grouping("kwh_tier").alias("is_total_kwh_tier"),
        F.min("voltage").alias("min_voltage"),
    )
)
