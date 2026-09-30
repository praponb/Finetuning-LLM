from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w6 = Window.partitionBy(F.month("started_at"), "country").orderBy(F.col("session_id").desc())
w7 = Window.partitionBy(F.month("started_at"), "country").orderBy(F.col("session_id").desc(), "id")
w8 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("browser").orderBy("started_at", "id")

with_windows = (
    df_sessions
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
)

with_gaps = (
    with_windows
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "first_known_browser",
        "last_known_browser",
        F.sum("new_group_flag").over(w4).alias("session_id"),
    )
)

ranked = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w6).alias("session_id_dense_rank"),
        F.row_number().over(w7).alias("seq_no"),
        F.avg("session_id").over(w8).alias("session_id_avg_2_prior"),
        F.first("country", ignorenulls=True).over(w9).alias("first_known_country"),
    )
)

flagged = (
    scored
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w10) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("session_start_flag").over(w9))

remaining_keys = (
    sessionized
    .select("browser", "id")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id").isin([8, 16, 22, 39]))
        .select("browser", "id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    reconciled
    .select("browser")
    .subtract(
        df_sessions
        .filter(F.col("started_at") >= "2025-01-02")
        .select("browser")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

wide = (
    verified
    .groupBy("country")
    .pivot("browser", ["Firefox", "Chrome"])
    .agg(F.max("session_id_avg_2_prior"))
)

result = (
    wide
    .select("country", "Firefox", "Chrome")
    .orderBy(F.col("country").desc())
)
