from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("duration_sec").asc_nulls_first())

unpacked = (
    df_app_logs
    .select(
        "id",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context").getItem("version").alias("version"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "version",
        F.last("duration_sec", ignorenulls=True).over(w).alias("last_known_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w).alias("last_known_duration_sec_2"),
        F.percent_rank().over(w2).alias("duration_sec_percent_rank"),
    )
)
