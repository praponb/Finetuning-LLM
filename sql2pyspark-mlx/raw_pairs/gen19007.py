from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("customer_id", "device").orderBy("years_since_started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy(F.to_date("started_at"), "device")
w5 = Window.partitionBy(F.month("event_time"), "browser").orderBy("next_years_since_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-10").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        (F.datediff(F.current_date(), "started_at") / 365).alias("years_since_started_at"),
        F.concat_ws("-", "browser", "country").alias("browser_country_key"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec") == 442), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"))
)

with_windows = (
    with_latest
    .withColumn("next_years_since_started_at", F.lead("years_since_started_at").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "years_since_started_at",
        "browser_country_key",
        "event_time",
        "next_years_since_started_at",
        "row_seq",
        F.row_number().over(w3).alias("seq_no"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w4)), 4).alias("row_seq_share"),
        F.sum("years_since_started_at").over(w5).alias("running_years_since_started_at"),
    )
)
