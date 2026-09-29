from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("YEAR", "started_at"), "browser").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy(F.date_trunc("YEAR", "started_at"), "browser")
w4 = Window.partitionBy("device").orderBy("started_at", "id")
w5 = Window.partitionBy("browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_hist = Window.orderBy("row_seq", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w8 = Window.orderBy(F.col("web_event_duration_sec").asc_nulls_first())
w9 = Window.orderBy("row_seq_share", "id")
w10 = Window.partitionBy("device").orderBy("web_event_duration_sec", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-23").cast("date")) & F.hour("started_at").between(8, 16))
    .withColumn("browser_upper", F.upper("browser"))
    .withColumn("country_clean", F.coalesce("country", F.lit("OTHER")))
)

scored = (
    cleaned
    .filter(F.col("device") == "mobile")
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "customer_id",
        "device",
        "browser_upper",
        "country_clean",
        "first_known_country",
        "row_seq",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w3)), 4).alias("row_seq_share"),
        F.lag("row_seq", 1, 0).over(w4).alias("prev_row_seq"),
        F.max("row_seq").over(w5).alias("row_seq_max_7d"),
        F.lag("row_seq", 1).over(w_hist).alias("prev_row_seq_2"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser_upper",
        "s.row_seq",
        "s.row_seq_share",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "browser_upper",
        "row_seq_share",
        "web_event_duration_sec",
        F.dense_rank().over(w8).alias("web_event_duration_sec_dense_rank"),
        F.row_number().over(w9).alias("row_seq_2"),
        F.avg("row_seq").over(w10).alias("row_seq_avg_5"),
    )
)

result = (
    with_windows
    .withColumn("row_seq_2_tier", F.when(F.col("row_seq_2") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("web_event_duration_sec_tier", F.when(F.col("web_event_duration_sec") >= 432, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["row_seq_2_tier", "web_event_duration_sec_tier"], ["row_seq_2_tier"]],
        "row_seq_2_tier",
        "web_event_duration_sec_tier",
    )
    .agg(
        F.percentile_approx("row_seq_avg_5", [0.25, 0.5, 0.75]).alias("row_seq_avg_5_quartiles"),
        F.max("row_seq_2").alias("max_row_seq_2"),
        F.percentile_approx("row_seq_avg_5", 0.95).alias("row_seq_avg_5_p95"),
    )
    .filter(F.col("max_row_seq_2") > 0)
    .orderBy(F.col("row_seq_2_tier").asc_nulls_last())
)
