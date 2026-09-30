from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-07-22 00:00:00").cast("timestamp"))
    .withColumn("device_country", F.concat("device", F.lit("/"), "country"))
)

with_last = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.browser", "s.country", F.col("c.event_time"))
)

base_sessions = (
    with_last
    .filter(F.col("country").isNotNull())
    .withColumn("country_prefix", F.substring("country", 1, 3))
    .withColumn("browser_country_key", F.concat_ws("-", "browser", "country"))
    .withColumn("country_filled", F.coalesce("country", F.lit("OTHER")))
)

eligible = (
    base_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([15, 23])), "left_anti")
)

matched_keys = (
    eligible
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("country", "browser")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

eligible_sessions = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.event_time")), "left_semi")
)

kept = (
    eligible_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type") == "add_to_cart"), "left_anti")
)

result = (
    kept
    .select("id", "browser", "event_time", "browser_country_key")
    .orderBy(F.col("browser").desc(), "id")
)
