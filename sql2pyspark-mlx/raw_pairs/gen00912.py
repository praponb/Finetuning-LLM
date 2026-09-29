from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.orderBy(F.col("is_add_to_cart").desc(), "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("is_add_to_cart").asc_nulls_first())
w4 = Window.partitionBy("page", "session_id").orderBy("is_add_to_cart", "id")
w5 = Window.partitionBy("page")
w6 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

latest = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    latest
    .filter(F.col("event_type") == "purchase")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "latest_rank",
        F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0).alias("is_add_to_cart"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

with_windows = (
    base
    .withColumn("is_add_to_cart_bucket", F.ntile(5).over(w2))
    .withColumn("is_add_to_cart_dense_rank", F.dense_rank().over(w3))
    .withColumn("is_add_to_cart_bucket_2", F.ntile(3).over(w4))
)

kept_keys = (
    with_windows
    .select("id", "session_id")
    .exceptAll(
        df_web_events
        .filter(F.to_date("event_time") < "2023-07-01")
        .select("id", "session_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("is_add_to_cart") < 47)
    .select(
        "session_id",
        "page",
        "event_time",
        "is_add_to_cart",
        "page_clean",
        "is_add_to_cart_bucket",
        "is_add_to_cart_dense_rank",
        "is_add_to_cart_bucket_2",
        F.round((F.col("is_add_to_cart_bucket_2") / F.sum("is_add_to_cart_bucket_2").over(w5)), 4).alias("is_add_to_cart_bucket_2_share"),
        F.avg("is_add_to_cart_dense_rank").over(w6).alias("is_add_to_cart_dense_rank_avg_30d"),
    )
)

result = (
    scored
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "page",
        "event_time",
        "is_add_to_cart",
        "is_add_to_cart_bucket_2",
        "is_add_to_cart_dense_rank_avg_30d",
        F.col("min_web_event_duration_sec"),
    )
)
