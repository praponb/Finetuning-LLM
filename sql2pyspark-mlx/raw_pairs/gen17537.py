from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.date_trunc("MONTH", "log_ts"), "service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    unpacked
    .filter(F.col("tier").isin(["free", "pro", "gold"]))
    .select(
        "id",
        "service",
        "level",
        "region_or_default",
        "duration_text",
        "version",
        F.last("tier", ignorenulls=True).over(w_hist).alias("last_known_tier"),
        F.last("version", ignorenulls=True).over(w2).alias("last_known_version"),
        F.first("tier", ignorenulls=True).over(w3).alias("first_known_tier"),
    )
)
