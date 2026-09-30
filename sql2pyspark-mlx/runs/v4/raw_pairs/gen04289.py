from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("session_no").desc())
w4 = Window.partitionBy("service", "level").orderBy("session_no")
w5 = Window.partitionBy("key").orderBy(F.col("log_ts_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

base = (
    exploded
    .filter((F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp")) & F.col("key").isNotNull() & ~F.col("level").isin(["ERROR", "INFO"]))
    .withColumn("log_ts_period", F.date_trunc("MONTH", "log_ts"))
    .withColumn("key_filled", F.coalesce("key", F.lit("NONE")))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

ranked = (
    sessionized
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    ranked
    .select("service", "id")
    .intersect(
        df_app_logs
        .filter(F.col("log_ts") < "2024-01-01")
        .select("service", "id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("session_no_dense_rank", F.dense_rank().over(w3))
    .withColumn("session_no_rank", F.rank().over(w4))
    .withColumn("session_no_avg_7d", F.avg("session_no").over(w5))
)

result = (
    enriched
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "log_ts_period",
        "key_filled",
        "session_no",
        "row_num",
        "session_no_dense_rank",
    )
    .orderBy(F.col("key").desc_nulls_last())
)
