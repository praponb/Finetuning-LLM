from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy(F.col("event_type_length").desc(), "id")
w6 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

with_windows = (
    df_web_events
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w))
    .withColumn("duration_sec_avg_30d", F.avg("duration_sec").over(w2))
    .withColumn("first_known_page", F.first("page", ignorenulls=True).over(w3))
)

base = (
    with_windows
    .filter(F.col("event_time") >= F.lit("2024-02-19").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "duration_sec_cume_dist",
        "first_known_page",
        F.length("event_type").alias("event_type_length"),
        F.when(F.col("duration_sec_cume_dist") >= 0.88, "High").when(F.col("duration_sec_cume_dist") >= 0.35, "Medium").otherwise("Low").alias("duration_sec_cume_dist_level"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.device"))
)

with_windows_web_events = (
    with_dims
    .withColumn("event_type_length_avg_30d", F.avg("event_type_length").over(w4))
    .withColumn("event_type_length_bucket", F.ntile(4).over(w5))
    .withColumn("duration_sec_max_7d", F.max("duration_sec").over(w6))
)

with_dims_web_events = (
    with_windows_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"), F.col("p.browser"))
)

kept_keys = (
    with_dims_web_events
    .select("session_id")
    .subtract(
        df_web_events
        .filter(F.col("event_type").isin(["search", "purchase"]))
        .select("session_id")
    )
)

reconciled = (
    with_dims_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "first_known_page",
        "device",
        "event_type_length_bucket",
        "duration_sec_max_7d",
        "country",
    )
)
