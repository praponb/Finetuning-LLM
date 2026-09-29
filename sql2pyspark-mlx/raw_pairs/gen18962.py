from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("log_ts"), "attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy(F.col("row_seq").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    flattened
    .filter((F.col("log_ts") > F.lit("2025-06-01 07:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "log_ts",
        "attr_key",
        "attr_value",
        F.substring("service", 1, 2).alias("service_prefix"),
        F.coalesce("attr_key", F.lit("N/A")).alias("resolved_attr_key"),
    )
)

scored = (
    valid
    .select(
        "id",
        "attr_key",
        "resolved_attr_key",
        F.row_number().over(w).alias("row_seq"),
        F.last("attr_key", ignorenulls=True).over(w2).alias("last_known_attr_key"),
        F.row_number().over(w).alias("event_seq"),
        F.first("attr_key", ignorenulls=True).over(w2).alias("first_known_attr_key"),
        F.first("attr_key", ignorenulls=True).over(w3).alias("first_known_attr_key_2"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "attr_key",
        "last_known_attr_key",
        "event_seq",
        "first_known_attr_key",
    )
)

matched_keys = (
    ranked
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("level") == "INFO")
        .select("id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    verified
    .groupBy(
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.max("event_seq").alias("max_event_seq"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
)

result = (
    stats
    .select("event_seq_tier", "region_events", "distinct_ids", "num_attr_keys")
    .orderBy(F.col("event_seq_tier").asc_nulls_last())
)
