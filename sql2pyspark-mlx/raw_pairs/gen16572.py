from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy(F.col("running_count_duration_sec").desc_nulls_last())

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-03-25 06:00:00").cast("timestamp"))
    .withColumn("duration_sec_whole", F.floor("duration_sec"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 489, "High").when(F.col("duration_sec") >= 114, "Medium").otherwise("Low"))
)

scored = (
    filtered
    .withColumn("running_sum_duration_sec", F.sum("duration_sec").over(w_ordered))
    .withColumn("running_count_duration_sec", F.count("duration_sec").over(w_ordered))
    .withColumn("running_max_duration_sec", F.max("duration_sec").over(w_ordered))
)

with_dims = (
    scored
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_time",
        "a.duration_sec_tier",
        "a.running_sum_duration_sec",
        "a.running_count_duration_sec",
        F.col("p.browser"),
        F.col("p.country"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.device"))
)

latest = (
    joined
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

stats = (
    latest
    .withColumn("running_sum_duration_sec_tier", F.when(F.col("running_sum_duration_sec") >= 2150, "HIGH").otherwise("LOW"))
    .cube("country", "running_sum_duration_sec_tier")
    .agg(
        F.min("running_count_duration_sec").alias("min_running_count_duration_sec"),
        F.sum(F.when(F.col("device") == "desktop", F.col("running_sum_duration_sec"))).alias("desktop_total_running_sum_duration_sec"),
        F.percentile_approx("running_sum_duration_sec", 0.5).alias("running_sum_duration_sec_p50"),
        F.min("event_time").alias("first_seen"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "country",
        "running_sum_duration_sec_tier",
        "desktop_total_running_sum_duration_sec",
        "running_sum_duration_sec_p50",
        "first_seen",
        "grouping_id",
        F.when(F.col("country").rlike("^(JP|TH)$"), "GROUP_A").when(F.col("country").rlike("^(US|SG)$"), "CORE").otherwise("EXTENDED").alias("country_zone"),
    )
)
