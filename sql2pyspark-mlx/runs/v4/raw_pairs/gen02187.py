from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_last())
w2 = Window.partitionBy("k", "level").orderBy("log_ts", "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

unpacked = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "k",
        "value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

ranked = (
    unpacked
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .withColumn("status_code_high_tier", F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["level", "status_code_high_tier"], ["level"]],
        "level",
        "status_code_high_tier",
    )
    .agg(
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
    )
    .filter(F.col("warn_events") > 1)
    .orderBy(F.col("level").desc())
)
