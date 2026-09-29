from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_hist = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("duration_sec", "id")

flattened_attrs = (
    df_app_logs
    .filter(F.to_date("log_ts") < "2024-11-23")
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .withColumn("status_code_count_2h", F.count("status_code").over(w_grp))
    .withColumn("retry_count_sum_30m", F.sum("retry_count").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("retry_count_avg_2h", F.avg("retry_count").over(w_grp))
)
