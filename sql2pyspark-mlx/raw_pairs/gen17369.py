from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "customer_id").orderBy(F.col("is_new_session").desc_nulls_first(), "id")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("device")
w6 = Window.partitionBy("country").orderBy("device_count_session_no", "id")

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

latest = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "device", "started_at", "country", "session_no")
)

kept = (
    latest
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec") == 177), "left_anti")
)

with_windows = (
    kept
    .select(
        "id",
        "country",
        "session_no",
        F.avg("session_no").over(w4).alias("country_avg_session_no"),
        F.count("session_no").over(w5).alias("device_count_session_no"),
    )
)

filtered = (
    with_windows
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "country",
        "device_count_session_no",
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
        F.substring("country", 1, 4).alias("country_prefix"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

scored = (
    filtered
    .withColumn("prev_is_us", F.lag("is_us").over(w6))
    .withColumn("prev_device_count_session_no", F.lag("device_count_session_no").over(w6))
    .withColumn("seq_no", F.row_number().over(w6))
)

result = (
    scored
    .select(
        "id",
        "device_count_session_no",
        "is_us",
        "country_prefix",
        "country_clean",
        "prev_is_us",
        "prev_device_count_session_no",
    )
    .orderBy("prev_is_us")
)
