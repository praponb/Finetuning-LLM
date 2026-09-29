from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy(F.to_date("event_time")).orderBy(F.col("duration_sec").asc_nulls_last())
w3 = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w_hist = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("page").orderBy(F.col("duration_sec_rank_2").desc())
w7 = Window.partitionBy(F.month("event_time"), "page")
w8 = Window.partitionBy(F.to_date("event_time"))
w9 = Window.partitionBy(F.to_date("event_time")).orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-06-18 22:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("page_event_type", F.concat("page", F.lit(" "), "event_type"))
)

kept_keys = (
    valid
    .select("page")
    .intersectAll(
        df_web_events
        .filter(F.col("event_type") != "purchase")
        .select("page")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.country"))
)

metrics = (
    enriched
    .withColumn("country_stddev_samp_duration_sec", F.stddev_samp("duration_sec").over(w))
    .withColumn("duration_sec_rank", F.rank().over(w2))
    .withColumn("duration_sec_rank_2", F.rank().over(w3))
    .withColumn("duration_sec_sum_30d", F.sum("duration_sec").over(w_hist))
)

metrics_web_events = (
    metrics
    .select(
        "id",
        "session_id",
        "event_time",
        "page_event_type",
        "country_stddev_samp_duration_sec",
        "duration_sec_rank_2",
        "duration_sec_sum_30d",
        F.sum("duration_sec").over(w5).alias("duration_sec_sum_90d"),
        F.cume_dist().over(w6).alias("duration_sec_rank_2_cume_dist"),
        F.stddev_samp("country_stddev_samp_duration_sec").over(w7).alias("event_time_month_stddev_samp_country_stddev_samp_duration_sec"),
        F.max("country_stddev_samp_duration_sec").over(w8).alias("event_time_to_date_max_country_stddev_samp_duration_sec"),
        F.max("duration_sec").over(w9).alias("duration_sec_max_30d"),
    )
)

joined = (
    metrics_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.device") == "tablet")
    .select(
        "e.id",
        "e.session_id",
        "e.country_stddev_samp_duration_sec",
        "e.duration_sec_rank_2",
        "e.duration_sec_max_30d",
        F.col("p.browser"),
    )
)

result = (
    joined
    .select("id", "duration_sec_rank_2", "browser")
    .orderBy(F.col("duration_sec_rank_2").asc_nulls_last())
)
