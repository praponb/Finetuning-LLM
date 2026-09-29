from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").asc_nulls_first(), "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-09-05 09:00:00"))) & F.hour("started_at").between(10, 13))
    .select(
        "*",
        F.coalesce("country", F.lit("unassigned")).alias("country_filled"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

prev_events = deduped.withColumn("prev_started_at", F.lag("started_at").over(w2))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.sum("is_new_session").over(w3).alias("burst_id"),
    )
)

remaining_keys = (
    islands
    .select("customer_id", "browser")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["DE", "JP", "SG"]))
        .select("customer_id", "browser")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.to_date(F.col("c.event_time")) < "2025-08-01"), "left_anti")
)

result = (
    not_flagged
    .cube("browser", "burst_id")
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.max(F.when(F.col("device") == "tablet", F.col("burst_id"))).alias("tablet_max_burst_id"),
    )
    .orderBy(F.col("tablet_max_burst_id").desc())
)
