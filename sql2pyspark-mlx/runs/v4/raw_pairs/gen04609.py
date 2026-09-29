from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("page")
w3 = Window.partitionBy("session_id").orderBy("duration_sec_max_90d")
w4 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

kept_keys = (
    df_web_events
    .select("id", "page")
    .subtract(
        df_web_events
        .filter(F.col("page") == "/home")
        .select("id", "page")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.max("duration_sec").over(w).alias("duration_sec_max_90d"),
        F.stddev_samp("duration_sec").over(w2).alias("page_stddev_samp_duration_sec"),
    )
)

metrics = (
    enriched
    .withColumn("duration_sec_max_90d_cume_dist", F.cume_dist().over(w3))
    .withColumn("duration_sec_max_90d_avg_7d", F.avg("duration_sec_max_90d").over(w4))
)

scored = (
    metrics
    .withColumn("duration_sec_avg_90d", F.avg("duration_sec").over(w5))
    .withColumn("duration_sec_max_90d_cume_dist_sum_90d", F.sum("duration_sec_max_90d_cume_dist").over(w))
)

result = (
    scored
    .select(
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_max_90d",
        "page_stddev_samp_duration_sec",
        "duration_sec_max_90d_cume_dist",
        "duration_sec_max_90d_avg_7d",
        "duration_sec_avg_90d",
    )
    .orderBy("duration_sec_max_90d_cume_dist")
)
