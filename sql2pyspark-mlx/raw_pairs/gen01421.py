from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("page").orderBy(F.col("browser_length").asc_nulls_first())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-07-17 00:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("country").alias("country_length"),
    )
)

valid = (
    base
    .filter((F.col("started_at") > F.to_date(F.lit("2023-07-07"))) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.coalesce("country_length", F.lit(0)).alias("country_length_filled"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

base_sessions = (
    valid
    .filter(F.col("browser") == "Firefox")
    .select(
        "id",
        "customer_id",
        "country",
        "browser_filled",
        F.length("browser").alias("browser_length"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled_2"),
    )
)

with_latest = (
    base_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.browser_length", "s.browser_filled_2", F.col("c.page"))
)

matched_keys = (
    with_latest
    .select("id")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = verified.withColumn("browser_length_dense_rank", F.dense_rank().over(w2))

not_flagged = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page") == "/home"), "left_semi")
)

result = (
    not_flagged
    .select("browser_length", "browser_filled_2", "browser_length_dense_rank")
    .orderBy(F.col("browser_length_dense_rank").desc())
)
