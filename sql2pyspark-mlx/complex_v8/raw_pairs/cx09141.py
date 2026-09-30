from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy("read_at_quarter", "id")

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage") <= 238.3) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

labeled = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "voltage_filled",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

deduped = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("kwh").alias("max_reading_kwh"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.min("kwh").alias("min_reading_kwh"),
    )
)

profiled = (
    deduped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_kwh",
        "m.avg_reading_kwh",
        "m.min_reading_kwh",
    )
)

result = (
    profiled
    .select("id", "avg_reading_kwh", "min_reading_kwh")
    .unpivot(
        ids=["id"],
        values=["avg_reading_kwh", "min_reading_kwh"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
