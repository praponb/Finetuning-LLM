from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id", "page").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("row_seq_2").desc(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/search"))))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

base_web_events = (
    base
    .filter(F.col("page").isNotNull() & F.col("session_id").isin([1, 17]))
    .select(
        "*",
        F.when(F.col("page").isin(["/checkout", "/product"]), "Priority").otherwise("Other").alias("page_group"),
    )
)

with_windows = (
    base_web_events
    .select(
        "id",
        "session_id",
        "page",
        "event_time_label",
        "page_filled",
        "page_group",
        F.row_number().over(w_ordered).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    latest
    .select("session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("page").isin(["/search", "/checkout", "/cart"]))
        .select("session_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("page", "page_filled", "page_group", "row_seq_2", "dedup_rank")
    .orderBy(F.col("row_seq_2").desc())
)
