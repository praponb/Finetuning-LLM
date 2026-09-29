from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code")
w2 = Window.partitionBy("host", "level").orderBy(F.col("status_code_cume_dist_rounded").desc_nulls_first())
w3 = Window.partitionBy("level")
w4 = Window.partitionBy(F.to_date("log_ts"), "host").orderBy("status_code_cume_dist")
w5 = Window.partitionBy(F.date_trunc("MONTH", "log_ts"))

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

filtered = (
    lines
    .filter(F.col("level").isin(["ERROR", "INFO", "WARN"]))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "k",
        "value",
        F.when(F.col("status_code") >= 451, "High").when(F.col("status_code") >= 236, "Medium").otherwise("Low").alias("status_code_band"),
    )
)

enriched = filtered.select("*", F.cume_dist().over(w).alias("status_code_cume_dist"))

cleaned = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "*",
        F.round("status_code_cume_dist", 2).alias("status_code_cume_dist_rounded"),
        (F.col("status_code") + F.col("status_code_cume_dist")).alias("status_code_plus_status_code_cume_dist"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "k",
        "value",
        "status_code_cume_dist_rounded",
        "status_code_plus_status_code_cume_dist",
        F.rank().over(w2).alias("status_code_cume_dist_rounded_rank"),
        F.stddev_samp("status_code_plus_status_code_cume_dist").over(w3).alias("level_stddev_samp_status_code_plus_status_code_cume_dist"),
        F.max("status_code_cume_dist").over(w3).alias("level_max_status_code_cume_dist"),
        F.dense_rank().over(w4).alias("status_code_cume_dist_dense_rank"),
        F.stddev_samp("status_code_plus_status_code_cume_dist").over(w5).alias("log_ts_period_stddev_samp_status_code_plus_status_code_cume_dist"),
    )
)
