from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy(F.month("event_time"), "device").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("duration_sec", "id")

ranked = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "session_id", "event_type", "event_time", "duration_sec")
)

joined = (
    ranked
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id") == 35)
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.event_time",
        "src.duration_sec",
        F.col("p.device"),
        F.col("p.country"),
        F.col("p.browser"),
    )
)

joined_web_events = (
    joined
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.country").alias("session_country"))
)

metrics = (
    joined_web_events
    .withColumn("duration_sec_avg_5", F.avg("duration_sec").over(w2))
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w3))
    .withColumn("next_duration_sec_2", F.lead("duration_sec", 2).over(w4))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("next_duration_sec_2") >= 539, "HIGH").otherwise("LOW").alias("next_duration_sec_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("session_country") == "US", 1)).alias("us_rows"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.sum(F.when(F.col("country") == "JP", F.col("next_duration_sec")).otherwise(0)).alias("jp_next_duration_sec"),
        F.count("*").alias("cnt"),
        F.round(F.avg("next_duration_sec"), 2).alias("avg_next_duration_sec"),
    )
    .filter(F.col("jp_next_duration_sec") > 0)
    .orderBy(F.col("next_duration_sec_2_tier").desc_nulls_last())
)
