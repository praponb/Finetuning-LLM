from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

summarized_rows = (
    cleaned.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

labeled = (
    summarized_rows
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "voltage",
        "quality",
        "read_at_hour",
        "reading_count",
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
    )
)

result = (
    labeled
    .select("id", "voltage_adj", "voltage")
    .unpivot(
        ids=["id"],
        values=["voltage_adj", "voltage"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
