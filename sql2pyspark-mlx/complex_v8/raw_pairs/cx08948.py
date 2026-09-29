from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_details = (
    scoped_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan") != "family"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
    )
)

tagged = (
    with_details
    .select(
        "*",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

derived = (
    tagged
    .select(
        "*",
        F.length(F.trim(F.col("is_skipped_label"))).alias("is_skipped_label_len"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.coalesce(F.col("listener_plan"), F.lit("UNKNOWN")).alias("listener_plan_or_unknown"),
    )
)

grouped = (
    derived
    .groupBy("is_skipped", "source")
    .agg(
        F.sum(F.when(F.col("listener_plan") == "student", 1).otherwise(0)).alias("n_listener_plan_student"),
        F.count("*").alias("cnt"),
        F.countDistinct("listener_country").alias("distinct_listener_countrys"),
        F.sum("is_skipped_flag").alias("total_is_skipped_flag"),
        F.sum("is_skipped_label_len").alias("total_is_skipped_label_len"),
    )
)

result = (
    grouped
    .select(
        "is_skipped",
        "source",
        "cnt",
        "total_is_skipped_flag",
        "total_is_skipped_label_len",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
    .orderBy(
        F.col("source_clean").desc(),
        F.col("total_is_skipped_label_len").desc(),
    )
)
