from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("host").orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

cleaned = (
    lines
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("status_code") > 273))
    .select(
        "id",
        "host",
        "status_code",
        "key",
        "value",
        F.concat_ws("/", "key", "host").alias("key_host_key"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "host",
        "status_code",
        "value",
        "key_host_key",
        F.percent_rank().over(w).alias("status_code_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_2"),
        F.percent_rank().over(w4).alias("status_code_percent_rank_2"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_3"),
    )
)

cleaned_app_logs = (
    metrics
    .filter(F.col("status_code") > 371)
    .select(
        "host",
        "status_code",
        "value",
        "status_code_percent_rank",
        "status_code_share",
        "status_code_share_2",
        "status_code_percent_rank_2",
        F.round("status_code_percent_rank_2", 2).alias("status_code_percent_rank_2_rounded"),
        F.round("status_code_share_2", 2).alias("status_code_share_2_rounded"),
    )
)

report = (
    cleaned_app_logs
    .select(
        "host",
        "status_code",
        "value",
        "status_code_percent_rank",
        "status_code_share_2",
        "status_code_percent_rank_2_rounded",
    )
)

result = (
    report
    .select(
        "host",
        "status_code",
        "value",
        "status_code_percent_rank",
        "status_code_share_2",
        "status_code_percent_rank_2_rounded",
        F.when(F.col("host").rlike("^(h1|h4)$"), "OTHER_ZONE").when(F.col("host").rlike("^(h2|h3)$"), "GROUP_B").otherwise("EXTENDED").alias("host_zone"),
    )
    .orderBy(F.col("status_code_percent_rank_2_rounded").desc())
)
