from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.orderBy("duration_sec", "id")
w4 = Window.partitionBy("page").orderBy("duration_sec", "id")
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy(F.year("event_time")).orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_avg_3").asc_nulls_first(), "id")
w8 = Window.partitionBy("event_type").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.year("event_time"))
w10 = Window.orderBy(F.col("event_seq").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-08-20").cast("date")) & F.col("page").isNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("duration_sec") >= 421, "High").when(F.col("duration_sec") >= 141, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest
    .filter(F.to_date("event_time") < "2023-08-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec_band",
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.lead("duration_sec", 2).over(w4).alias("next_duration_sec"),
        F.sum("duration_sec").over(w5).alias("duration_sec_sum_7d"),
        F.avg("duration_sec").over(w6).alias("duration_sec_avg_3"),
    )
)

latest_web_events = (
    metrics
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "next_duration_sec",
        "rn",
        F.last("event_seq", ignorenulls=True).over(w8).alias("last_known_event_seq"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w9)), 4).alias("event_seq_share"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"), F.col("p.device"))
)

filtered = (
    enriched
    .filter((~F.col("country").eqNullSafe(F.lit("DE"))) & (F.col("next_duration_sec") == 431))
    .withColumn("seq_no_minus_rn", (F.col("seq_no") - F.col("rn")))
)

wide = (
    filtered
    .groupBy("session_id")
    .pivot("country", ["TH", "JP"])
    .agg(F.sum("next_duration_sec").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    wide
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "TH_value_sum",
        "JP_value_sum",
        "JP_cnt",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy("TH_value_sum")
)
