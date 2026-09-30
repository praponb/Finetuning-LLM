from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "tier").orderBy(F.col("status_code_high").desc(), "id")
w_grp = Window.partitionBy("tier")
w3 = Window.partitionBy("level", "tier").orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.ntile(10).over(w).alias("status_code_high_bucket"),
        F.min("status_code_high").over(w_grp).alias("tier_min_status_code_high"),
        F.sum("status_code_high").over(w_grp).alias("tier_sum_status_code_high"),
        F.first("status_code_high", ignorenulls=True).over(w3).alias("first_known_status_code_high"),
    )
)
