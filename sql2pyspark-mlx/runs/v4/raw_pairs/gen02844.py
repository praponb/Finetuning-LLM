from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc_nulls_last())

latest = (
    df_web_events
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 528, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["session_id", "duration_sec_tier"], ["session_id"]],
        "session_id",
        "duration_sec_tier",
    )
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.count("*").alias("cnt"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("event_time").alias("first_seen"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("duration_sec_tier").asc_nulls_last())
)
