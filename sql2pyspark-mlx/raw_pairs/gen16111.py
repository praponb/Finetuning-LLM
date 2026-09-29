from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("log_ts").desc())
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "log_ts",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

latest = (
    unpacked
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_gaps = (
    latest
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "key",
        "attr_value",
        "status_code_high",
        "is_flagged",
        F.sum("is_new_session").over(w3).alias("island_id"),
    )
)

result = (
    sessionized
    .select(
        "id",
        "log_ts",
        "key",
        "attr_value",
        "status_code_high",
        "is_flagged",
        "island_id",
        F.when(F.col("key").rlike("^(region|version)$"), "GROUP_A").otherwise("CORE").alias("key_zone"),
    )
    .orderBy(F.col("key_zone").desc(), F.col("log_ts").desc())
)
