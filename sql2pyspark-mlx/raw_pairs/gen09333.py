from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.month("log_ts")).orderBy("status_code", "id").rowsBetween(-5, Window.currentRow)
w_part_2 = Window.partitionBy("service").orderBy("status_code", "id")
w_hist = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(-6, Window.currentRow)

unpacked = (
    df_app_logs
    .filter(F.col("status_code") > 330)
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        F.last("duration_sec", ignorenulls=True).over(w_ordered).alias("last_known_duration_sec"),
        F.avg("duration_sec").over(w_part).alias("duration_sec_avg_5"),
        F.sum("status_code").over(w_ordered).alias("cumulative_status_code"),
        F.lead("duration_sec", 1).over(w_part_2).alias("next_duration_sec"),
        F.avg("duration_sec").over(w_hist).alias("duration_sec_avg_6"),
    )
)
