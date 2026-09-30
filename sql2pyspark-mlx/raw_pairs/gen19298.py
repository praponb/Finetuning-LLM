from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("started_at"), "browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_part = Window.partitionBy("country", "device").orderBy(F.col("is_safari").asc_nulls_first())
w3 = Window.partitionBy("browser")

cleaned = (
    df_sessions
    .filter(F.col("customer_id") == 31)
    .select("*", F.substring("country", 1, 4).alias("country_prefix"))
)

kept_keys = (
    cleaned
    .select("device", "country")
    .subtract(
        df_sessions
        .filter(F.col("country") == "US")
        .select("device", "country")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= "2025-02-01"), "left_anti")
)

filtered = (
    not_flagged
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-04-04 14:00:00"))) & (~F.col("country").eqNullSafe(F.lit("UK"))))
    .select(
        "*",
        F.substring("country", 1, 3).alias("country_prefix_2"),
        F.when(F.col("browser") == "Safari", 1).otherwise(0).alias("is_safari"),
    )
)

metrics = (
    filtered
    .filter(F.col("is_safari") >= 37)
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "country_prefix",
        "country_prefix_2",
        "is_safari",
        F.avg("is_safari").over(w).alias("is_safari_avg_30d"),
        F.cume_dist().over(w_part).alias("is_safari_cume_dist"),
        F.avg("is_safari").over(w3).alias("browser_avg_is_safari"),
    )
)

result = (
    metrics
    .select(
        "id",
        "browser",
        "started_at",
        "country_prefix",
        "is_safari",
        "is_safari_avg_30d",
        "is_safari_cume_dist",
        "browser_avg_is_safari",
        F.when(F.col("browser").rlike("^(Edge|Chrome)$"), "GROUP_B").otherwise("GROUP_A").alias("browser_zone"),
    )
    .orderBy(F.col("is_safari_avg_30d").desc_nulls_last())
)
