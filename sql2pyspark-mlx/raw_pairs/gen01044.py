from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-06-17").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("device_browser_key", F.concat_ws("/", "device", "browser"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.page").alias("web_event_page"))
)

kept_keys = (
    with_last
    .select("customer_id", "id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([1, 30]))
        .select("customer_id", "id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([8, 19, 30])), "left_anti")
)

result = (
    kept
    .groupBy("device")
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("device").desc())
)
