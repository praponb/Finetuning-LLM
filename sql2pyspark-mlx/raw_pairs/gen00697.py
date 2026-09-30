from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("age").asc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy("age", "id")
w3 = Window.partitionBy(F.to_date("started_at"), "customer_id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("browser", "device").orderBy(F.col("next_age").desc_nulls_last(), "id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-03-22 11:00:00"))) & F.col("customer_id").isin([1, 2]))
    .select(
        "*",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.age"))
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "device",
        "browser",
        "country_prefix",
        "browser_clean",
        "age",
        "dedup_rank",
        F.lead("age").over(w2).alias("next_age"),
        F.row_number().over(w2).alias("seq_no"),
        F.max("age").over(w3).alias("started_at_to_date_max_age"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec") == 434), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser",
        "s.country_prefix",
        "s.browser_clean",
        "s.age",
        "s.dedup_rank",
        "s.next_age",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

latest = (
    with_latest
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("web_event_event_type")
    .pivot("browser", ["Firefox", "Edge", "Safari"])
    .agg(F.sum("age").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
