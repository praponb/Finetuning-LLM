from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-03-10").cast("date")) & F.col("page").isNotNull())
    .withColumn("months_since_event_time", (F.datediff(F.current_date(), "event_time") / 30))
    .withColumn("page_group", F.when(F.col("page").isin(["/search", "/home"]), "Tier 1").otherwise("Other"))
    .withColumn("page_filled", F.coalesce("page", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .filter(F.col("months_since_event_time") >= 35)
    .select(
        "id",
        "session_id",
        "event_time",
        "months_since_event_time",
        "page_filled",
        F.sum("duration_sec").over(w_part).alias("running_sum_duration_sec"),
        F.avg("duration_sec").over(w_part).alias("running_avg_duration_sec"),
        F.max("duration_sec").over(w_part).alias("running_max_duration_sec"),
    )
)
