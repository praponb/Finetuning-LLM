from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at_day").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("started_at_day_2").desc_nulls_first())
w3 = Window.partitionBy("device", "country")

base = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

matched_keys = (
    deduped
    .select("customer_id", "browser")
    .exceptAll(
        df_sessions
        .filter(F.col("country") == "DE")
        .select("customer_id", "browser")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("country").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Edge"))))
    .select(
        "device",
        "started_at",
        "country",
        F.dayofmonth("started_at").alias("started_at_day_2"),
        F.when(F.col("device").isin(["tablet", "desktop"]), "Priority").otherwise("Other").alias("device_group"),
    )
)

scored = (
    filtered
    .withColumn("started_at_day_2_dense_rank", F.dense_rank().over(w2))
    .withColumn("device_avg_started_at_day_2", F.avg("started_at_day_2").over(w3))
)

result = (
    scored
    .select(
        "device",
        "started_at",
        "country",
        "started_at_day_2",
        "device_group",
        "started_at_day_2_dense_rank",
    )
)
