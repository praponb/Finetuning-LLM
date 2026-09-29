from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "shipped_date"), "status")
w3 = Window.partitionBy("status").orderBy("shipped_date_minus_73d", "id")
w4 = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc_nulls_first())

base = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-05-30").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.upper("status").alias("status_upper"),
        F.date_sub("shipped_date", 73).alias("shipped_date_minus_73d"),
    )
)

enriched = (
    base
    .filter(F.col("cost") < 82)
    .select(
        "id",
        "carrier",
        F.lead("cost", 1, 0).over(w).alias("next_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.row_number().over(w3).alias("event_seq"),
        F.cume_dist().over(w4).alias("cost_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(F.col("carrier").isin(["UPS", "DHL", "Kerry"]))
    .select(
        "id",
        "cost_share",
        "event_seq",
        "cost_cume_dist",
        F.round((F.col("cost_share") / F.col("event_seq")), 2).alias("cost_share_per_event_seq"),
        F.ceil("event_seq").alias("event_seq_whole"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

matched_keys = (
    filtered
    .select("id")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date").between("2023-12-07", "2025-02-20"))
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("cost_share").between(0.35, 0.38))
    .select(
        "cost_share",
        "event_seq",
        "cost_share_per_event_seq",
        "event_seq_whole",
        "carrier_clean",
        F.least("event_seq_whole", "cost_share").alias("least_event_seq_whole_cost_share"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("event_seq_whole") >= 72, "HIGH").otherwise("LOW").alias("event_seq_whole_tier"),
    )
    .agg(
        F.round(F.avg("cost_share_per_event_seq"), 2).alias("avg_cost_share_per_event_seq"),
        F.round(F.avg("cost_share"), 2).alias("avg_cost_share"),
        F.round(F.avg("least_event_seq_whole_cost_share"), 2).alias("avg_least_event_seq_whole_cost_share"),
        F.percentile_approx("cost_share_per_event_seq", 0.95).alias("cost_share_per_event_seq_p95"),
        F.max("event_seq").alias("max_event_seq"),
    )
)
