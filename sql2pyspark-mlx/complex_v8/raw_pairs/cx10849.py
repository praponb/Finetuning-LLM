from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .filter((F.col("site_id") >= 7) | (F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.abs(F.col("voltage") - 229.1).alias("voltage_distance"),
    )
)

latest = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

screened = (
    latest.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-18 13:00:00").cast("timestamp")) & (F.col("m.voltage") >= 218.2), "left_anti")
    .filter(F.col("quality").isin("BAD", "OK"))
)

labeled = (
    screened
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_ref",
        "voltage_distance",
        F.coalesce(F.col("voltage_distance"), F.lit(0.0)).alias("voltage_distance_filled"),
        F.when(F.col("voltage_distance") >= 147.0, "HIGH").when(F.col("voltage_distance") >= 85.8, "MEDIUM").otherwise("LOW").alias("voltage_distance_tier"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

summarized_rows = (
    labeled.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

profiled = (
    summarized_rows.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    profiled
    .select("id", "kwh", "voltage")
    .unpivot(
        ids=["id"],
        values=["kwh", "voltage"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
