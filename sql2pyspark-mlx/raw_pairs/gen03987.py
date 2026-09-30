from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.month("event_time")).orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id")
w4 = Window.orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").asc_nulls_last())
w6 = Window.orderBy(F.col("duration_sec_avg_3").desc())
w7 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

scored = (
    df_web_events
    .select(
        "*",
        F.percent_rank().over(w_hist).alias("duration_sec_percent_rank"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_3"),
        F.stddev_samp("duration_sec").over(w3).alias("event_type_stddev_samp_duration_sec"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "page",
        "duration_sec",
        "duration_sec_percent_rank",
        "duration_sec_avg_3",
        "event_type_stddev_samp_duration_sec",
        F.row_number().over(w4).alias("row_seq"),
        F.cume_dist().over(w5).alias("duration_sec_cume_dist"),
        F.cume_dist().over(w6).alias("duration_sec_avg_3_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(F.col("page").isNotNull() & (F.col("duration_sec") != 196))
    .select(
        "id",
        "page",
        "duration_sec",
        "duration_sec_percent_rank",
        "duration_sec_avg_3",
        "event_type_stddev_samp_duration_sec",
        "row_seq",
        "duration_sec_cume_dist",
        "duration_sec_avg_3_cume_dist",
        F.round("event_type_stddev_samp_duration_sec", 1).alias("event_type_stddev_samp_duration_sec_rounded"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "page",
        "duration_sec_percent_rank",
        "duration_sec_cume_dist",
        "event_type_stddev_samp_duration_sec_rounded",
        "duration_sec_avg_3_cume_dist",
    )
    .unpivot(
        ids=["id", "page"],
        values=["duration_sec_percent_rank", "duration_sec_cume_dist", "event_type_stddev_samp_duration_sec_rounded", "duration_sec_avg_3_cume_dist"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
