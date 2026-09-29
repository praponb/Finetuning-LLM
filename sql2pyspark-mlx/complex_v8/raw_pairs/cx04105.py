from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device = Window.partitionBy("device")

scoped_plays = (
    df_plays
    .filter(F.col("track_id") <= 6)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.when(F.col("source").isin("album", "search"), "group_a").when(F.col("source") == "playlist", "group_b").otherwise("other").alias("source_group"),
    )
)

ranked = (
    scoped_plays
    .select(
        "*",
        F.min("ms_played").over(w_all_by_ms_played_running).alias("min_ms_played_so_far"),
        F.round(F.avg("ms_played").over(w_device), 2).alias("device_avg_ms_played"),
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
    )
)

scored = (
    ranked
    .filter(F.col("min_ms_played_so_far").between(604250, 2801500))
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "ms_played_filled",
        "source_group",
        "min_ms_played_so_far",
        "device_avg_ms_played",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "source",
        "ms_played_filled",
        "source_group",
        "device_avg_ms_played",
        "source_ref",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.round(F.col("device_avg_ms_played") * 1.07, 2).alias("device_avg_ms_played_adj"),
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

derived = (
    enriched
    .select(
        "*",
        F.when(F.col("source_group").isin("group_b", "other"), "group_a").when(F.col("source_group") == "group_a", "group_b").otherwise("other").alias("source_group_group"),
        F.round(F.col("device_avg_ms_played_adj") / F.nullif(F.col("days_since_played_at"), F.lit(0)), 3).alias("device_avg_ms_played_adj_per_days_since_played_a"),
        F.when(F.col("device_avg_ms_played_adj") >= 481000.0, "A").when(F.col("device_avg_ms_played_adj") >= 213250.0, "B").otherwise("C").alias("device_avg_ms_played_adj_tier"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.min("device_avg_ms_played").alias("overall_min_device_avg_ms_played")))
    .filter(F.col("device_avg_ms_played") > F.col("overall_min_device_avg_ms_played"))
    .drop("overall_min_device_avg_ms_played")
)

result = (
    screened
    .select(
        "id",
        F.col("days_since_played_at").alias("days_since_played_at_v"),
        F.col("ms_played_filled").alias("m_ms_played_filled"),
    )
    .unpivot(
        ids=["id"],
        values=["days_since_played_at_v", "m_ms_played_filled"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
