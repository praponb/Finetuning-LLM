from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id").orderBy(F.col("session_start_flag").desc_nulls_first())
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-16").cast("date")) & (~F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("page") == "/cart", 1).otherwise(0).alias("is_cart"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

kept_keys = (
    sessionized
    .select("id", "session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("session_id") == 27)
        .select("id", "session_id")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.dense_rank().over(w3).alias("session_start_flag_dense_rank"),
        F.max("is_cart").over(w4).alias("is_cart_max_7d"),
    )
)

result = (
    metrics
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .groupingSets(
        [["browser", "page"], ["browser"], []],
        F.col("d.browser").alias("browser"),
        F.col("src.page").alias("page"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.is_cart_max_7d")).alias("max_is_cart_max_7d"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("max_is_cart_max_7d").asc_nulls_last())
)
