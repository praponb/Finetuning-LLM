from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claim_approved_amount_minus_avg").desc(), F.col("id").desc(), F.col("claim_id").desc())
w_last_claim_reported_at_to_da = Window.partitionBy(F.to_date("last_claim_reported_at"))
w_all_by_stddev_avg_claim_appr = Window.orderBy(F.col("stddev_avg_claim_approved_amount").desc(), "claim_reported_at_ym")
w_all_by_stddev_avg_claim_appr_2 = Window.orderBy(F.col("stddev_avg_claim_approved_amount").desc(), "claim_reported_at_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") > 10518.4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
    )
)

exploded_detail_claims = (
    prepared_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.reported_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.reported_at",
        "s.status_claim_type_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

claim_status_stats = (
    exploded_detail_claims
    .groupBy("claim_status")
    .agg(F.avg("claim_approved_amount").alias("avg_claim_approved_amount"))
)

outliers = (
    exploded_detail_claims.alias("s")
    .join(claim_status_stats.alias("g"), F.col("g.claim_status") == F.col("s.claim_status"), "inner")
    .filter(F.col("s.claim_approved_amount") > F.col("g.avg_claim_approved_amount"))
    .select(
        "s.*",
        "g.avg_claim_approved_amount",
        F.round(F.col("s.claim_approved_amount") - F.col("g.avg_claim_approved_amount"), 2).alias("claim_approved_amount_minus_avg"),
    )
)

latest_per_key = (
    outliers
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

per_group = (
    latest_per_key
    .groupBy(
        F.date_format("claim_reported_at", "yyyy-MM").alias("claim_reported_at_ym"),
    )
    .agg(
        F.max("claim_approved_amount_minus_avg").alias("max_claim_approved_amount_minus_avg"),
        F.round(F.sum("claim_approved_amount"), 2).alias("total_claim_approved_amount"),
        F.round(F.stddev("avg_claim_approved_amount"), 2).alias("stddev_avg_claim_approved_amount"),
        F.round(F.sum("claim_approved_amount_minus_avg") / F.nullif(F.sum("claim_approved_amount"), F.lit(0)), 3).alias("claim_approved_amount_minus_avg_to_claim_approve"),
        F.max("claim_reported_at").alias("last_claim_reported_at"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        F.max("total_claim_approved_amount").over(w_last_claim_reported_at_to_da).alias("last_claim_reported_at_to_date_max_total_claim_a"),
        F.round((F.unix_timestamp("last_claim_reported_at") - F.unix_timestamp(F.lag("last_claim_reported_at").over(w_all_by_stddev_avg_claim_appr))) / 60, 1).alias("minutes_since_prev_last_claim_reported_at"),
        F.first("total_claim_approved_amount", ignorenulls=True).over(w_all_by_stddev_avg_claim_appr_2).alias("first_known_total_claim_approved_amount"),
    )
)

result = (
    with_windows
    .select(
        "claim_reported_at_ym",
        "max_claim_approved_amount_minus_avg",
        "total_claim_approved_amount",
        "stddev_avg_claim_approved_amount",
        "claim_approved_amount_minus_avg_to_claim_approve",
        "last_claim_reported_at",
        "last_claim_reported_at_to_date_max_total_claim_a",
        "minutes_since_prev_last_claim_reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("claim_reported_at_ym")), " ", "_")).alias("claim_reported_at_ym_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("last_claim_reported_at")).alias("days_since_last_claim_reported_at"),
    )
    .orderBy(
        F.col("stddev_avg_claim_approved_amount").desc(),
        "total_claim_approved_amount",
    )
)
