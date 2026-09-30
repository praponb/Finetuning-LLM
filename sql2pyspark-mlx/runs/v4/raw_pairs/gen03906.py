from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2024-01-03"))) & F.col("country").isNotNull())
    .select("*", F.concat_ws("-", "device", "country").alias("device_country_key"))
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

with_last = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

kept = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

with_dims = (
    kept
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.age"))
)

remaining_keys = (
    with_dims
    .select("id")
    .subtract(
        df_sessions
        .filter(F.col("device") != "tablet")
        .select("id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id") == 33), "left_anti")
)

result = (
    not_flagged
    .withColumn("age_tier", F.when(F.col("age") >= 66, "HIGH").otherwise("LOW"))
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 8, "HIGH").otherwise("LOW"))
    .rollup("age_tier", "island_id_tier")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.max("web_event_event_time").alias("ended_at"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("island_id_tier")
)
