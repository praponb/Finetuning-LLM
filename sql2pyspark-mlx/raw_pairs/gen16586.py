from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w3 = Window.orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("level").orderBy(F.col("status_code_cume_dist").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("status_code").between(379, 433))
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "key",
        "attr_value",
        F.col("status_code").cast("string").alias("status_code_str"),
        F.trim("host").alias("clean_host"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "level",
        "status_code",
        "clean_host",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
    )
)

enriched = (
    with_windows
    .filter(F.col("status_code_cume_dist") <= 0.5)
    .withColumn("level_count_status_code_cume_dist", F.count("status_code_cume_dist").over(w))
)

with_windows_app_logs = (
    enriched
    .withColumn("status_code_rank", F.rank().over(w3))
    .withColumn("status_code_cume_dist_cume_dist", F.cume_dist().over(w4))
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("status_code_cume_dist_cume_dist") >= 0.61, "LARGE").otherwise("SMALL").alias("status_code_cume_dist_cume_dist_tier"),
        "level",
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
    )
)
