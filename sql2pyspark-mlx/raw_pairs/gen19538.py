from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

latest = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

kept = (
    latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

kept_sessions = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

latest_match = (
    kept_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.customer_id", F.col("c.page"))
)

remaining_keys = (
    latest_match
    .select("id", "customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("id", "customer_id")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("page") != "/checkout")
    .select(
        "*",
        F.when(F.col("page") == "/checkout", 1).otherwise(0).alias("is_checkout"),
        F.coalesce("page", F.lit("Unknown")).alias("page_filled"),
    )
)

result = (
    cleaned
    .select("id", "customer_id", "page", "is_checkout", "page_filled")
    .orderBy(F.col("is_checkout").desc())
)
