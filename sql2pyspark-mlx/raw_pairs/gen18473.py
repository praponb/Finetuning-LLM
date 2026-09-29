from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "tier").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("service", "host")
w4 = Window.partitionBy("tier").orderBy("duration_sec")
w5 = Window.partitionBy(F.to_date("log_ts"), "service").orderBy("duration_sec", "id")

parsed = (
    df_app_logs
    .filter(F.col("host").isin(["h3", "h1"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .select(
        "id",
        "service",
        "log_ts",
        "tier",
        "region_or_default",
        "duration_sec",
        F.first("region_or_default", ignorenulls=True).over(w).alias("first_known_region_or_default"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6_prior"),
        F.avg("duration_sec").over(w3).alias("service_avg_duration_sec"),
        F.percent_rank().over(w4).alias("duration_sec_percent_rank"),
        F.ntile(10).over(w5).alias("duration_sec_bucket"),
    )
)
