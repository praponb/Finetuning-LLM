from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy(F.year("event_time")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy(F.col("event_time").asc_nulls_last())
w5 = Window.partitionBy(F.date_trunc("MONTH", "event_time"), "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy(F.year("event_time"), "session_id").orderBy("cumulative_duration_sec", "id")
w8 = Window.partitionBy("session_id").orderBy(F.col("cumulative_duration_sec_avg_2").desc_nulls_first())

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-03-14 05:00:00").cast("timestamp"))
    .select(
        "*",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.when(F.col("event_type").isin(["add_to_cart", "purchase"]), "Group A").otherwise("Other").alias("event_type_group"),
    )
)

enriched = (
    filtered
    .withColumn("prev_duration_sec", F.lag("duration_sec", 1).over(w))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w3))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "event_type_title",
        "event_type_group",
        "prev_duration_sec",
        "cumulative_duration_sec",
        "next_duration_sec",
    )
)

metrics = (
    ranked
    .filter(F.col("duration_sec") != 277)
    .select(
        "*",
        F.sum("cumulative_duration_sec").over(w5).alias("cumulative_cumulative_duration_sec"),
        F.avg("cumulative_duration_sec").over(w6).alias("cumulative_duration_sec_avg_2"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.event_time",
        "e.event_type_group",
        "e.next_duration_sec",
        "e.cumulative_duration_sec_avg_2",
        "e.row_seq",
        F.col("d.device"),
    )
)

ranked_web_events = (
    with_dims
    .withColumn("rn", F.rank().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "event_time",
        "next_duration_sec",
        "cumulative_duration_sec_avg_2",
        "row_seq",
        "device",
    )
)

result = (
    ranked_web_events
    .groupBy("device")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.percentile_approx("next_duration_sec", [0.25, 0.5, 0.75]).alias("next_duration_sec_quartiles"),
    )
)
