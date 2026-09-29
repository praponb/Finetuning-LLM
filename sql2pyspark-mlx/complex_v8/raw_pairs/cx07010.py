from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("kwh") <= 116.8) | (F.col("site_id") <= 6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

qualified = (
    prepared
    .crossJoin(prepared.agg(F.max("voltage").alias("overall_max_voltage")))
    .filter(F.col("voltage") < F.col("overall_max_voltage"))
    .drop("overall_max_voltage")
)

annotated = (
    qualified
    .select(
        "*",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj"),
    )
)

result = (
    annotated
    .groupBy("read_at")
    .pivot("quality", ["OK", "BAD", "EST"])
    .agg(F.avg("voltage_adj"))
    .orderBy("read_at")
)
