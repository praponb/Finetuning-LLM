from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w3 = Window.partitionBy("event_type")
w4 = Window.partitionBy(F.month("event_time")).orderBy(F.col("duration_sec_count_90d").desc_nulls_first())
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-04-15 14:00:00"))) & (F.col("page").eqNullSafe(F.lit("/product"))))
    .select(
        "*",
        F.when(F.col("duration_sec") >= 538, "High").when(F.col("duration_sec") >= 107, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

remaining_keys = (
    base
    .select("event_type", "page")
    .exceptAll(
        df_web_events
        .filter(F.col("event_time") >= "2023-06-27")
        .select("event_type", "page")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.count("duration_sec").over(w).alias("duration_sec_count_90d"),
        F.rank().over(w2).alias("duration_sec_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.count("duration_sec_count_90d").over(w3).alias("event_type_count_duration_sec_count_90d"),
        F.dense_rank().over(w4).alias("duration_sec_count_90d_dense_rank"),
        F.max("duration_sec_rank").over(w5).alias("duration_sec_rank_max_90d"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .filter(F.col("d.customer_id").isin([29, 33, 34]))
    .select("src.*", F.col("d.country"), F.col("d.device"), F.col("d.browser"))
)

result = (
    joined
    .groupBy(
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        F.when(F.col("duration_sec_count_90d_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("duration_sec_count_90d_dense_rank_tier"),
        "device",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.count_if(F.col("event_type") == "click").alias("click_events"),
        F.percentile_approx("duration_sec_count_90d_dense_rank", [0.25, 0.5, 0.75]).alias("duration_sec_count_90d_dense_rank_quartiles"),
    )
    .filter(F.col("purchase_rows") > 1)
)
