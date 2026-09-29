from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("event_time"), "page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "event_time"), "event_type").orderBy("event_time", "id")
w3 = Window.orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("page", "session_id").orderBy("duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy(F.col("duration_sec").asc_nulls_last(), "id")

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .filter(F.col("d.customer_id").isin([16, 23, 25, 35]))
    .select("e.*", F.col("d.browser"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.ntile(5).over(w).alias("duration_sec_bucket"),
        F.lead("duration_sec", 1).over(w2).alias("next_duration_sec"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "duration_sec",
        "browser",
        "next_duration_sec",
        "event_seq",
        F.sum("duration_sec_bucket").over(w4).alias("running_duration_sec_bucket"),
        F.max("event_seq").over(w5).alias("event_seq_max_7d"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.round(F.sum("running_duration_sec_bucket") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("running_duration_sec_bucket_per_chrome"),
        F.count_if(F.col("browser") == "Safari").alias("safari_events"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .orderBy(F.col("safari_events").desc_nulls_first())
)
