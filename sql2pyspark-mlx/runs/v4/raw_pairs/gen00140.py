from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("tier").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("status_code", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("tier", "host").orderBy("retry_count", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .withColumn("host_min_retry_count", F.min("retry_count").over(w))
    .withColumn("last_known_retry_count", F.last("retry_count", ignorenulls=True).over(w2))
    .withColumn("retry_count_avg_3_prior", F.avg("retry_count").over(w3))
    .withColumn("next_status_code", F.lead("status_code", 2).over(w4))
)
