from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("quality") != "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

screened = (
    filtered_readings
    .crossJoin(filtered_readings.agg(F.min("voltage").alias("overall_min_voltage")))
    .filter(F.col("voltage") > F.col("overall_min_voltage"))
    .drop("overall_min_voltage")
)

result = (
    screened
    .groupBy("site_id", "quality_len")
    .pivot("quality", ["BAD", "OK"])
    .agg(F.max("kwh"))
)
