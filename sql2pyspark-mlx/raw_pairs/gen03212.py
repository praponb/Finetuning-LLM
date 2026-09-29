from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device").orderBy("is_chrome", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "browser").orderBy(F.col("running_min_is_chrome").desc())
w3 = Window.partitionBy("device").orderBy("cumulative_is_chrome", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy(F.col("is_chrome").asc_nulls_first())

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.to_date(F.lit("2023-07-10")))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_filled"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type") == "page_view"), "left_anti")
)

metrics = (
    eligible
    .filter(~F.col("country").isin(["DE", "SG", "JP"]))
    .withColumn("running_max_is_chrome", F.max("is_chrome").over(w_recent))
    .withColumn("running_avg_is_chrome", F.avg("is_chrome").over(w_recent))
    .withColumn("running_min_is_chrome", F.min("is_chrome").over(w_recent))
    .withColumn("cumulative_is_chrome", F.sum("is_chrome").over(w_recent))
)

with_windows = (
    metrics
    .select(
        "id",
        "device",
        "is_chrome",
        F.dense_rank().over(w2).alias("running_min_is_chrome_dense_rank"),
        F.first("running_min_is_chrome", ignorenulls=True).over(w3).alias("first_known_running_min_is_chrome"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "device", "first_known_running_min_is_chrome")
)

eligible_sessions = (
    latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec") == 216), "left_anti")
)

valid = (
    eligible_sessions
    .filter(F.col("device") != "mobile")
    .withColumn("first_known_running_min_is_chrome_whole", F.floor("first_known_running_min_is_chrome"))
)

report = (
    valid
    .select(
        "id",
        "device",
        "first_known_running_min_is_chrome",
        F.when(F.col("device").rlike("^(mobile|desktop)$"), "CORE").when(F.col("device").rlike("^(tablet)$"), "GROUP_A").otherwise("GROUP_B").alias("device_zone"),
    )
)

result = (
    report
    .select("device", "device_zone")
    .orderBy(F.col("device").asc_nulls_last())
)
