from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("flag_bit8", "id")
w2 = Window.partitionBy("tier").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .filter(F.col("host").isin(["h1", "h4", "h3"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "region_or_default",
        F.lead("flag_bit8", 2, 0).over(w).alias("next_flag_bit8"),
        F.last("latency_ms", ignorenulls=True).over(w2).alias("last_known_latency_ms"),
    )
)
