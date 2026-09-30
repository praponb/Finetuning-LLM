from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("event_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy("duration_sec", "id")
w3 = Window.orderBy("duration_sec", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("page", "device").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.orderBy("duration_sec", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("country").orderBy("event_time", "id")
w7 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"), F.col("p.device"), F.col("p.browser"))
)

valid = (
    enriched
    .filter((F.col("event_time") > F.lit("2023-11-28 14:00:00").cast("timestamp")) & F.col("browser").isNotNull() & (F.col("browser") == "Safari"))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Safari", "Edge"]), "Tier 1").otherwise("Other"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("duration_sec_avg_6_prior", F.avg("duration_sec").over(w3))
    .withColumn("duration_sec_sum_7d", F.sum("duration_sec").over(w4))
    .withColumn("duration_sec_avg_5_prior", F.avg("duration_sec").over(w5))
)

flagged = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("burst_id", F.sum("is_new_session").over(w7))

result = (
    sessions_web_events
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "device",
        "browser_group",
        "latest_rank",
        "duration_sec_avg_6_prior",
    )
    .orderBy("duration_sec")
)
