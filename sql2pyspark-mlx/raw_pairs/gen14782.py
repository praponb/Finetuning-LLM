from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "key")
w2 = Window.partitionBy(F.month("log_ts"), "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("level", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

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

ranked = (
    exploded
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "service",
        "key",
        "value",
        "dedup_rank",
        F.count("status_code").over(w2).alias("status_code_count_1h"),
        F.max("status_code").over(w3).alias("status_code_max_1h"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.to_date("log_ts") < "2024-01-01")
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "key",
        "value",
        "dedup_rank",
        "status_code_count_1h",
        "status_code_max_1h",
    )
    .orderBy(F.col("status_code_count_1h").desc_nulls_first())
)
