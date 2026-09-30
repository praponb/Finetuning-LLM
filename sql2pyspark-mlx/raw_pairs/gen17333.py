from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

gaps = (
    not_flagged
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

valid = (
    sessionized
    .filter((F.col("country").eqNullSafe(F.lit("US"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "new_group_flag",
        "burst_id",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.started_at_label",
        F.col("c.page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

filtered = (
    with_latest
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "device",
        "started_at_label",
        "page",
        "web_event_duration_sec",
        F.length("page").alias("page_length"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

result = (
    filtered
    .withColumn("page_length_tier", F.when(F.col("page_length") >= 61, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["device", "page", "page_length_tier"], ["device", "page"], ["device"]],
        "device",
        "page",
        "page_length_tier",
    )
    .agg(
        F.min("page_length").alias("min_page_length"),
        F.sum(F.when(F.col("page") == "/product", F.col("web_event_duration_sec")).otherwise(0)).alias("product_web_event_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("page_length_tier").asc_nulls_first())
)
