from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "v"),
    )
)

valid = (
    lines
    .filter(F.col("log_ts") >= F.lit("2025-06-01 08:00:00").cast("timestamp"))
    .select("*", F.concat_ws(",", "level", "host").alias("level_host_key"))
)

ranked = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    ranked
    .filter(F.col("attr_key").isin(["region", "version", "trace"]))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "attr_key",
        "v",
        "level_host_key",
        F.trim("level").alias("clean_level"),
        F.col("rn").cast("double").alias("rn_num"),
    )
)

result = (
    filtered
    .select(
        "id",
        "level",
        "attr_key",
        "v",
        "level_host_key",
        "clean_level",
        "rn_num",
        F.when(F.col("level").rlike("^(ERROR|DEBUG)$"), "GROUP_B").when(F.col("level").rlike("^(INFO|WARN)$"), "OTHER_ZONE").otherwise("CORE").alias("level_zone"),
    )
    .orderBy(F.col("attr_key").desc())
)
