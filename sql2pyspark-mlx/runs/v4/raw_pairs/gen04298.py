from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "v"),
    )
)

kept_keys = (
    exploded
    .select("level")
    .intersect(
        df_app_logs
        .filter(F.col("level").isin(["WARN", "DEBUG", "INFO"]))
        .select("level")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("log_ts") >= F.lit("2025-06-02").cast("date")) & F.col("key").isNotNull() & (F.col("key") != "region"))
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "v",
        F.concat("key", F.lit(", "), "host").alias("key_host"),
        F.when(F.col("level").isin(["DEBUG", "ERROR"]), "Priority").otherwise("Other").alias("level_group"),
    )
)

filtered = (
    cleaned
    .filter(F.col("level") == "INFO")
    .withColumn("host_length", F.length("host"))
)

result = (
    filtered
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.round(F.avg("host_length"), 2).alias("avg_host_length"),
        F.sum("host_length").alias("total_host_length"),
    )
    .orderBy("total_host_length")
)
