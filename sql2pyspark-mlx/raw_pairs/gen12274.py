from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc())
w2 = Window.partitionBy("k").orderBy("is_flagged")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "latency_ms",
        "k",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

kept_keys = (
    parsed
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("level").isin(["ERROR", "DEBUG"]))
        .select("id")
    )
)

kept_rows = (
    parsed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "latency_ms",
        "is_flagged",
        F.percent_rank().over(w).alias("status_code_high_percent_rank"),
        F.rank().over(w2).alias("is_flagged_rank"),
    )
)

result = (
    enriched
    .select(
        "latency_ms",
        "is_flagged",
        "status_code_high_percent_rank",
        "is_flagged_rank",
    )
    .orderBy(F.col("latency_ms").desc())
)
