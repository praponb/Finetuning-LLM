from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "level", F.explode("context").alias("attr_key", "v"))
)

valid = (
    lines
    .filter(F.col("attr_key").isNull())
    .withColumn("attr_key_title", F.initcap(F.trim("attr_key")))
    .withColumn("service_prefix", F.substring("service", 1, 3))
)

valid_app_logs = (
    valid
    .filter(F.col("service").isin(["billing", "checkout", "search"]))
    .withColumn("attr_key_level", F.concat("attr_key", F.lit("/"), "level"))
    .withColumn("service_title", F.initcap(F.trim("service")))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

valid_app_logs_2 = (
    valid_app_logs
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "attr_key",
        "v",
        "attr_key_title",
        "attr_key_level",
        "service_title",
        "attr_key_filled",
        F.lower("attr_key").alias("attr_key_lower"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

cleaned = (
    valid_app_logs_2
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "attr_key",
        "attr_key_title",
        "attr_key_level",
        "resolved_attr_key",
        F.when(F.col("service").isin(["checkout", "auth"]), "Priority").otherwise("Other").alias("service_group"),
    )
)

matched_keys = (
    cleaned
    .select("service", "id")
    .intersect(
        df_app_logs
        .filter(F.hour("log_ts").between(10, 17))
        .select("service", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("attr_key")
    .agg(
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.countDistinct("service").alias("distinct_services"),
        F.size(F.collect_set("service")).alias("num_services_2"),
    )
    .filter(F.col("distinct_attr_keys") >= 1)
    .orderBy("num_attr_keys")
)
