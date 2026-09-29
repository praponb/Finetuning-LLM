from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_median_csat = Window.partitionBy("agent_id", "csat").orderBy(F.col("median_csat").desc())
w_requester_id_tier_agent_id = Window.partitionBy("requester_id_tier", "agent_id")
w_agent_id_csat_by_median_csat_2 = Window.partitionBy("agent_id", "csat").orderBy(F.col("median_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-19 21:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
        F.when(F.col("requester_id") >= 20, "HIGH").when(F.col("requester_id") >= 8, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
    )
)

agent_id_stats = (
    prepared
    .groupBy("agent_id")
    .agg(F.percentile_approx("csat", 0.5).alias("median_csat"))
)

outliers = (
    prepared.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") >= F.col("g.median_csat"))
    .select(
        "s.*",
        "g.median_csat",
        F.round(F.col("s.csat") - F.col("g.median_csat"), 2).alias("csat_minus_median"),
    )
)

metrics = (
    outliers
    .select(
        "*",
        F.rank().over(w_agent_id_csat_by_median_csat).alias("median_csat_rank"),
        F.min("csat_minus_median").over(w_requester_id_tier_agent_id).alias("requester_id_tier_agent_id_min_csat_minus_median"),
        F.first("opened_at").over(w_agent_id_csat_by_median_csat_2).alias("first_opened_at"),
        F.nth_value("opened_at", 2).over(w_agent_id_csat_by_median_csat_2).alias("second_opened_at"),
        F.round(F.col("csat") - F.avg("csat").over(w_requester_id_tier_agent_id), 2).alias("csat_vs_requester_id_tier_agent_id_avg"),
    )
)

result = (
    metrics
    .groupingSets(
        [["requester_id_tier", "agent_id", "csat"], ["requester_id_tier"], []],
        "requester_id_tier", "agent_id", "csat",
    )
    .agg(
        F.grouping("requester_id_tier").alias("is_total_requester_id_tier"),
        F.min("median_csat_rank").alias("min_median_csat_rank"),
    )
)
