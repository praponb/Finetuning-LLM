from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time"), "browser")
w2 = Window.partitionBy("country", "page").orderBy("event_time", "id")
w3 = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").asc_nulls_last())
w_hist = Window.partitionBy("country").orderBy("duration_sec_rank", "id")
w5 = Window.partitionBy("browser")

with_dims = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.device") == "mobile")
    .select("a.*", F.col("b.browser"), F.col("b.device"), F.col("b.country"))
)

valid = (
    with_dims
    .filter(F.col("page").eqNullSafe(F.lit("/product")))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "browser",
        "device",
        "country",
        F.lower("browser").alias("browser_lower"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("event_time") > F.to_timestamp(F.lit("2023-07-17 15:00:00")))
    .select(
        "*",
        F.lower("page").alias("page_lower"),
        F.when(F.col("event_type") == "page_view", 1).otherwise(0).alias("is_page_view"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.max("is_page_view").over(w).alias("event_time_year_max_is_page_view"),
        F.lag("is_page_view", 1).over(w2).alias("prev_is_page_view"),
        F.rank().over(w3).alias("duration_sec_rank"),
    )
)

metrics = (
    scored
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("browser_sum_duration_sec_rank", F.sum("duration_sec_rank").over(w5))
)

result = (
    metrics
    .groupBy(F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("distinct_countries") > 1)
    .orderBy(F.col("duration_min").desc_nulls_first())
)
