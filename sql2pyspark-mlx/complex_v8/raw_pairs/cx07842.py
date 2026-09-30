from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.col("csat") >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

rollup_base = (
    scoped
    .groupBy("channel", "csat")
    .agg(
        F.max("closed_at").alias("last_closed_at"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.round(F.avg("requester_id"), 2).alias("avg_requester_id"),
        F.min("requester_id").alias("min_requester_id"),
    )
)

derived = (
    rollup_base
    .filter(F.col("n_rows").between(4, 11))
    .select(
        "channel",
        "csat",
        "last_closed_at",
        "median_csat",
        "avg_requester_id",
        "min_requester_id",
        F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW").alias("csat_tier"),
        F.date_format(F.col("last_closed_at"), "yyyy-MM").alias("last_closed_at_label"),
    )
)

derived_2 = (
    derived
    .select(
        "channel",
        "csat",
        "median_csat",
        "min_requester_id",
        "last_closed_at_label",
        F.length(F.trim(F.col("last_closed_at_label"))).alias("last_closed_at_label_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_closed_at")).alias("days_since_last_closed_at"),
    )
)

screened = (
    derived_2
    .crossJoin(derived_2.agg(F.avg("median_csat").alias("overall_avg_median_csat")))
    .filter(F.col("median_csat") > F.col("overall_avg_median_csat"))
    .drop("overall_avg_median_csat")
)

derived_3 = (
    screened
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.least(F.col("min_requester_id"), F.col("last_closed_at_label_len")).alias("least_min_requester_id_last_closed_at_label_len"),
        (F.col("last_closed_at_label_len") + F.col("median_csat")).alias("last_closed_at_label_len_plus_median_csat"),
    )
)

result = (
    derived_3
    .groupBy("median_csat")
    .pivot("channel", ["chat", "phone", "email", "web"])
    .agg(F.sum("least_min_requester_id_last_closed_at_label_len"))
    .orderBy("median_csat")
)
