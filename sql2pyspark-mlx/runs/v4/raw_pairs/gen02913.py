from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy("duration_sec", "id")
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w6 = Window.partitionBy("session_id").orderBy("duration_sec_bucket", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("event_type").orderBy(F.col("duration_sec_bucket").desc_nulls_first())
w8 = Window.orderBy(F.col("duration_sec").desc(), "id")
w9 = Window.partitionBy("country").orderBy("duration_sec_rounded_max_7d_avg_2", "id")
w10 = Window.partitionBy("country").orderBy(F.col("cumulative_duration_sec_rounded").desc_nulls_first())
w11 = Window.partitionBy("country").orderBy(F.col("duration_sec").desc())

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("*", F.round("duration_sec", 2).alias("duration_sec_rounded"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.max("duration_sec_rounded").over(w2).alias("duration_sec_rounded_max_7d"),
        F.ntile(4).over(w3).alias("duration_sec_bucket"),
        F.sum("duration_sec_rounded").over(w4).alias("cumulative_duration_sec_rounded"),
        F.ntile(5).over(w5).alias("duration_sec_bucket_2"),
    )
)

with_windows_web_events = (
    with_windows
    .select(
        "*",
        F.avg("duration_sec_rounded_max_7d").over(w6).alias("duration_sec_rounded_max_7d_avg_2"),
        F.percent_rank().over(w_part).alias("duration_sec_bucket_percent_rank"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

with_dims = (
    with_windows_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.device"), F.col("d.country"))
)

base = (
    with_dims
    .filter((F.col("event_time") > F.lit("2024-01-29").cast("date")) & F.col("country").isNotNull() & F.col("cumulative_duration_sec_rounded").between(167, 320))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "rn",
        "duration_sec_rounded_max_7d",
        "duration_sec_bucket",
        "cumulative_duration_sec_rounded",
        "duration_sec_rounded_max_7d_avg_2",
        "event_seq",
        "country",
        F.when(F.col("cumulative_duration_sec_rounded") >= 412, "High").when(F.col("cumulative_duration_sec_rounded") >= 66, "Medium").otherwise("Low").alias("cumulative_duration_sec_rounded_level"),
        F.initcap(F.trim("page")).alias("page_title"),
    )
)

scored = (
    base
    .select(
        "*",
        F.ntile(4).over(w9).alias("duration_sec_rounded_max_7d_avg_2_bucket"),
        F.cume_dist().over(w10).alias("cumulative_duration_sec_rounded_cume_dist"),
        F.dense_rank().over(w11).alias("duration_sec_dense_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("cumulative_duration_sec_rounded") >= 325, "PREMIUM").otherwise("STANDARD").alias("cumulative_duration_sec_rounded_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("duration_sec_bucket"), 2).alias("avg_duration_sec_bucket"),
        F.max(F.when(F.col("country") == "DE", F.col("duration_sec_dense_rank"))).alias("de_max_duration_sec_dense_rank"),
    )
)
