from pyspark.sql import functions as F

flattened = df_app_logs.select("id", "host", "level", F.explode("context").alias("k", "value"))

kept_keys = (
    flattened
    .select("host", "level")
    .exceptAll(
        df_app_logs
        .filter(F.hour("log_ts").between(12, 20))
        .select("host", "level")
    )
)

verified = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.trim("host").alias("clean_host"),
        F.concat("host", F.lit(", "), "k").alias("host_k"),
    )
)

valid = (
    base
    .filter(F.col("k").isNotNull())
    .withColumn("clean_level", F.trim("level"))
)

cleaned = (
    valid
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("region"))))
    .withColumn("k_lower", F.lower("k"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("OTHER")))
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("host").isin(["h1", "h3"]))
    .select(
        "id",
        "host",
        "level",
        "k",
        "clean_host",
        "host_k",
        "clean_level",
        "k_lower",
        F.lower("k").alias("k_lower_2"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

result = (
    cleaned_app_logs
    .groupBy("k", "level")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count("*").alias("event_count_2"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("event_count_2").asc_nulls_first())
)
