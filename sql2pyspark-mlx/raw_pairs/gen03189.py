from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("k", "host")
w4 = Window.partitionBy("k")
w_recent = Window.orderBy(F.col("status_code_avg_2h_max_30m").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

scored = flattened.select("*", F.avg("status_code").over(w).alias("status_code_avg_2h"))

filtered = (
    scored
    .filter(F.col("log_ts") >= F.to_date(F.lit("2025-06-01")))
    .select("*", F.trim("host").alias("clean_host"))
)

with_windows = (
    filtered
    .filter(F.col("status_code_avg_2h") >= 356)
    .select(
        "status_code",
        "k",
        "value",
        "status_code_avg_2h",
        "clean_host",
        F.max("status_code_avg_2h").over(w2).alias("status_code_avg_2h_max_30m"),
        F.max("status_code").over(w3).alias("k_max_status_code"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "value",
        "status_code_avg_2h",
        "clean_host",
        "status_code_avg_2h_max_30m",
        "k_max_status_code",
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share"),
        F.dense_rank().over(w_recent).alias("status_code_avg_2h_max_30m_dense_rank"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("status_code_share") >= 0.81, "LARGE").otherwise("SMALL").alias("status_code_share_tier"),
    )
    .agg(
        F.percentile_approx("status_code_avg_2h_max_30m_dense_rank", [0.25, 0.5, 0.75]).alias("status_code_avg_2h_max_30m_dense_rank_quartiles"),
        F.percentile_approx("status_code_avg_2h_max_30m", [0.25, 0.5, 0.75]).alias("status_code_avg_2h_max_30m_quartiles"),
        F.avg("k_max_status_code").alias("avg_k_max_status_code"),
    )
    .orderBy("status_code_share_tier")
)
