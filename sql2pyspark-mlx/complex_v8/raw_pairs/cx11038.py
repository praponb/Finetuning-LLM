from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_kwh_distance = Window.orderBy(F.col("total_kwh_distance").desc(), "reading_id", "reading_quality")
w_row_count_reading_quality = Window.partitionBy("row_count", "reading_quality")
w_all_by_row_count = Window.orderBy(F.col("row_count").asc_nulls_last(), "reading_id", "reading_quality")

recent_readings = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("kwh") >= 52.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.abs(F.col("kwh") - 81.1).alias("kwh_distance"),
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

detail_rows = (
    recent_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality_slug",
        "s.kwh_distance",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

summary = (
    detail_rows
    .filter(F.col("reading_voltage").between(218.5, 233.6))
    .groupBy("reading_id", "reading_quality")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("kwh_distance"), 2).alias("total_kwh_distance"),
    )
)

with_windows = (
    summary
    .select(
        "reading_id",
        "reading_quality",
        "row_count",
        "total_kwh_distance",
        F.rank().over(w_all_by_total_kwh_distance).alias("total_kwh_distance_rank"),
        F.max("total_kwh_distance").over(w_row_count_reading_quality).alias("row_count_reading_quality_max_total_kwh_distance"),
        F.dense_rank().over(w_all_by_row_count).alias("row_count_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "reading_id",
        "reading_quality",
        "row_count",
        "total_kwh_distance",
        "total_kwh_distance_rank",
        "row_count_reading_quality_max_total_kwh_distance",
        "row_count_dense_rank",
    )
    .orderBy(F.col("row_count_reading_quality_max_total_kwh_distance").asc())
)
