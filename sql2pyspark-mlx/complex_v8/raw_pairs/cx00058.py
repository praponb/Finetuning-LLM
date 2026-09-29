from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_hour = Window.partitionBy("read_at_hour")
w_read_at_hour_by_read_at_hour = Window.partitionBy("read_at_hour").orderBy("read_at_hour", "id")
w_latest = Window.partitionBy("site_id").orderBy(F.col("read_at_hour_min_voltage").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

cleaned = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

without_matches = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 113.1), "left_semi")
)

with_trends = (
    without_matches
    .select(
        "*",
        F.min("voltage").over(w_read_at_hour).alias("read_at_hour_min_voltage"),
        F.size(F.collect_set("site_id").over(w_read_at_hour)).alias("distinct_site_id_in_read_at_hour"),
        F.ntile(3).over(w_read_at_hour_by_read_at_hour).alias("read_at_hour_tercile"),
    )
)

deduped = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches_2 = (
    deduped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")) & (F.col("m.quality") == "EST"), "left_anti")
)

latest = (
    without_matches_2
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .filter(F.col("read_at_hour_tercile") <= 9)
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_hour_min_voltage",
        "distinct_site_id_in_read_at_hour",
        "read_at_hour_tercile",
        F.lower(F.trim(F.col("quality_slug"))).alias("quality_slug_clean"),
    )
    .orderBy(F.col("voltage").desc())
)
