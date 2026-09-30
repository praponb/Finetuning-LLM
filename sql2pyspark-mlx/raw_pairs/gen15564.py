from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("burst_id", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("year_start").orderBy(F.col("span_minutes").desc())

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-08-09").cast("date"))
    .select(
        "*",
        F.lower("event_type").alias("event_type_lower"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

cleaned_web_events = (
    cleaned
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.substring("page", 1, 4).alias("page_prefix"),
    )
)

gaps = (
    cleaned_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        "page_clean",
        "duration_sec_filled",
        "page_prefix",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

metrics = (
    sessionized
    .withColumn("burst_id_avg_6_prior", F.avg("burst_id").over(w3))
    .withColumn("cumulative_duration_sec_filled", F.sum("duration_sec_filled").over(w2))
)

result = (
    metrics
    .groupBy(
        F.date_trunc("year", "event_time").alias("year_start"),
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.max("event_time").alias("last_seen"),
        F.round(F.sum("duration_sec_filled") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("duration_sec_filled_per_cart"),
        F.sum(F.when(F.col("page") == "/search", F.col("burst_id")).otherwise(0)).alias("search_burst_id"),
    )
    .withColumn("span_minutes_percent_rank", F.percent_rank().over(w4))
)
