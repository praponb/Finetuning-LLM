from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_by_last_read = Window.partitionBy(F.to_date("read_at")).orderBy("last_reading_read_at")
w_reading_quality_count = Window.partitionBy("reading_quality_count")
w_read_at_to_date_by_last_read_2 = Window.partitionBy(F.to_date("read_at")).orderBy("last_reading_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.count(F.when(F.col("quality") != "BAD", 1)).alias("reading_quality_count"),
    )
)

with_counts = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

with_windows = (
    with_counts
    .select(
        "id",
        "kwh",
        "voltage",
        "read_at_label",
        "quality_or_unknown",
        "quality_prefix",
        "last_reading_read_at",
        "reading_quality_count",
        F.dense_rank().over(w_read_at_to_date_by_last_read).alias("last_reading_read_at_dense_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_reading_quality_count), F.lit(0)), 4).alias("voltage_share_of_reading_quality_count"),
        F.first("last_reading_read_at").over(w_read_at_to_date_by_last_read_2).alias("first_last_reading_read_at"),
    )
)

result = (
    with_windows
    .select("id", "last_reading_read_at_dense_rank", "reading_quality_count")
    .unpivot(
        ids=["id"],
        values=["last_reading_read_at_dense_rank", "reading_quality_count"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
