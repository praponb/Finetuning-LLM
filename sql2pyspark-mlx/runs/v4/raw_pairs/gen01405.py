from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("duration_sec")
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_part = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-12-25 17:00:00").cast("timestamp")) & F.col("page").isNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("page").alias("page_lower"),
        F.trim("event_type").alias("clean_event_type"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    ranked
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w2))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w_part))
    .withColumn("last_known_duration_sec", F.last("duration_sec", ignorenulls=True).over(w_hist))
)

with_dims = (
    scored
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.country"), F.col("b.device"))
)

matched_keys = (
    with_dims
    .select("session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") <= 271)
        .select("session_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .rollup(
        "country",
        F.when(F.col("duration_sec_sum_90d") >= 2634, "PREMIUM").otherwise("STANDARD").alias("duration_sec_sum_90d_tier"),
        F.when(F.col("cumulative_duration_sec") >= 2677, "HIGH").otherwise("LOW").alias("cumulative_duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("last_known_duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("last_known_duration_sec_per_cart"),
        F.min("event_time").alias("started_at"),
    )
)

result = (
    stats
    .select(
        "country",
        "duration_sec_sum_90d_tier",
        "cumulative_duration_sec_tier",
        "last_known_duration_sec_per_cart",
        F.when(F.col("country").rlike("^(US|DE)$"), "PRIORITY").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc_nulls_first())
)
