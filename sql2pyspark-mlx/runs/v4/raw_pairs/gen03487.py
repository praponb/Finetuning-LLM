from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_flagged").desc())
w2 = Window.partitionBy("service").orderBy("is_flagged")
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("level", "key")
w5 = Window.partitionBy("level", "key").orderBy("is_flagged")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    exploded
    .select(
        "service",
        "level",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

filtered = (
    unpacked
    .filter(F.col("key").isNotNull() & (F.col("level") != "DEBUG"))
    .select(
        "service",
        "level",
        "key",
        "status_code_high",
        "is_flagged",
        F.substring("key", 1, 2).alias("key_prefix"),
        F.trim("level").alias("clean_level"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

scored = (
    filtered
    .withColumn("is_flagged_dense_rank", F.dense_rank().over(w))
    .withColumn("is_flagged_percent_rank", F.percent_rank().over(w2))
    .withColumn("key_count_is_flagged", F.count("is_flagged").over(w3))
)

matched_keys = (
    scored
    .select("service", "level")
    .exceptAll(
        df_app_logs
        .filter(F.col("level") == "DEBUG")
        .select("service", "level")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

scored_app_logs = (
    reconciled
    .select(
        "*",
        F.max("is_flagged").over(w4).alias("level_max_is_flagged"),
        F.max("is_flagged_percent_rank").over(w4).alias("level_max_is_flagged_percent_rank"),
        F.rank().over(w5).alias("is_flagged_rank"),
    )
)

grouped = (
    scored_app_logs
    .groupBy(
        F.when(F.col("level_max_is_flagged") >= 1, "HIGH").otherwise("LOW").alias("level_max_is_flagged_tier"),
        F.when(F.col("level_max_is_flagged_percent_rank") >= 1, "LARGE").otherwise("SMALL").alias("level_max_is_flagged_percent_rank_tier"),
        "service",
    )
    .agg(
        F.count_if(F.col("key") == "trace").alias("trace_events"),
        F.percentile_approx("is_flagged_dense_rank", [0.25, 0.5, 0.75]).alias("is_flagged_dense_rank_quartiles"),
        F.percentile_approx("is_flagged", [0.25, 0.5, 0.75]).alias("is_flagged_quartiles"),
    )
)

result = (
    grouped
    .select(
        "level_max_is_flagged_tier",
        "level_max_is_flagged_percent_rank_tier",
        "service",
        "trace_events",
        "is_flagged_dense_rank_quartiles",
        "is_flagged_quartiles",
    )
)
