from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy(F.year("log_ts"), "service").orderBy("log_ts", "id", "attr_key")
w_grp_2 = Window.partitionBy(F.year("log_ts"), "attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w_recent = Window.partitionBy(F.year("log_ts"), "attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy(F.col("event_seq").asc_nulls_last())
w5 = Window.partitionBy(F.to_date("log_ts"), "attr_key")
w6 = Window.partitionBy(F.date_trunc("week", "log_ts"), "attr_key")
w7 = Window.partitionBy("attr_key")
w8 = Window.partitionBy("service", "attr_key").orderBy(F.col("log_ts").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "value"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "attr_key",
        F.row_number().over(w_grp).alias("event_seq"),
        F.row_number().over(w_grp_2).alias("event_seq_2"),
        F.last("attr_key", ignorenulls=True).over(w_recent).alias("last_known_attr_key"),
    )
)

base = (
    metrics
    .filter(F.col("attr_key").isNotNull() & (F.col("log_ts") > F.lit("2025-06-01 07:00:00").cast("timestamp")))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("Unknown")))
    .withColumn("service_attr_key", F.concat("service", F.lit(" "), "attr_key"))
)

scored = (
    base
    .filter(F.col("event_seq_2") < 5)
    .select(
        "*",
        F.cume_dist().over(w4).alias("event_seq_cume_dist"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4).alias("event_seq_share"),
        F.stddev_samp("event_seq").over(w6).alias("log_ts_period_stddev_samp_event_seq"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w6)), 4).alias("event_seq_share_2"),
        F.count("event_seq_2").over(w7).alias("attr_key_count_event_seq_2"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.rank().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .withColumn("log_ts_period_stddev_samp_event_seq_tier", F.when(F.col("log_ts_period_stddev_samp_event_seq") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["activity_day", "log_ts_period_stddev_samp_event_seq_tier"], ["activity_day"], []],
        "activity_day",
        "log_ts_period_stddev_samp_event_seq_tier",
    )
    .agg(
        F.min("log_ts").alias("started_at"),
        F.max("log_ts").alias("ended_at"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("activity_day").desc_nulls_last())
)
