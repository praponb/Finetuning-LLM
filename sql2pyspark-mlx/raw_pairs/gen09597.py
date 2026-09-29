from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec_rounded").desc())
w2 = Window.partitionBy("event_type").orderBy("duration_sec_rounded", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.year("event_time"), "page").orderBy("duration_sec", "id")
w4 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_avg_3_prior").desc(), "id")
w5 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded_cume_dist").desc(), "id")
w6 = Window.partitionBy("event_type").orderBy("duration_sec_rounded_cume_dist", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("event_type").orderBy("next_duration_sec_rounded", "id")
w8 = Window.orderBy(F.col("duration_sec_rounded_cume_dist").asc_nulls_last())

base = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/search", "/cart"]))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 2))
)

enriched = (
    base
    .select(
        "id",
        "session_id",
        "event_type",
        F.cume_dist().over(w).alias("duration_sec_rounded_cume_dist"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_3_prior"),
        F.lead("duration_sec_rounded", 2).over(w3).alias("next_duration_sec_rounded"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked_web_events = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "event_type",
        "duration_sec_rounded_cume_dist",
        "duration_sec_avg_3_prior",
        "next_duration_sec_rounded",
    )
)

scored = (
    ranked_web_events
    .filter(F.col("next_duration_sec_rounded").between(22, 31))
    .select(
        "*",
        F.avg("duration_sec_avg_3_prior").over(w6).alias("duration_sec_avg_3_prior_avg_3_prior"),
        F.lag("duration_sec_rounded_cume_dist", 1).over(w7).alias("prev_duration_sec_rounded_cume_dist"),
        F.percent_rank().over(w8).alias("duration_sec_rounded_cume_dist_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_type",
        "duration_sec_rounded_cume_dist",
        "duration_sec_rounded_cume_dist_percent_rank",
    )
    .unpivot(
        ids=["id", "event_type"],
        values=["duration_sec_rounded_cume_dist", "duration_sec_rounded_cume_dist_percent_rank"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
