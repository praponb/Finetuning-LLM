from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy("reported_at", "id")
w_reported_at_month_policy_id = Window.partitionBy("reported_at_month", "policy_id").orderBy(F.col("reported_at_month").asc_nulls_last(), "id")
w_reported_at_month_policy_id_2 = Window.partitionBy("reported_at_month", "policy_id").orderBy(F.col("reported_at_month").asc_nulls_last())
w_policy_id_adjuster_id_by_rep = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("reported_at_date").desc(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")

cleaned = (
    df_claims
    .filter(F.col("approved_amount") <= 23120.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.month(F.col("reported_at")).alias("reported_at_month"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_month",
        "s.reported_at_date",
        "s.claimed_amount_bucket",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

unique_rows = (
    combined_details
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    unique_rows
    .select(
        "*",
        F.lead("reported_at").over(w_reported_at_month_policy_id).alias("next_reported_at"),
        F.rank().over(w_reported_at_month_policy_id_2).alias("reported_at_month_rank"),
        F.dense_rank().over(w_policy_id_adjuster_id_by_rep).alias("reported_at_date_dense_rank"),
        F.round(F.col("claimed_amount_bucket") - F.avg("claimed_amount_bucket").over(w_adjuster_id), 2).alias("claimed_amount_bucket_vs_adjuster_id_avg"),
    )
)

summary = (
    metrics
    .groupBy("adjuster_id", "policy_id")
    .agg(
        F.max("reported_at_date").alias("last_reported_at_date"),
        F.percentile_approx("reported_at_month", 0.5).alias("median_reported_at_month"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
    .filter(F.col("cnt") >= 3)
)

without_matches = (
    summary.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount").between(0.0, 23487.5)), "left_semi")
)

result = (
    without_matches
    .filter(F.col("cnt") < 8)
    .select(
        "adjuster_id",
        "policy_id",
        "last_reported_at_date",
        "median_reported_at_month",
        F.date_trunc("month", F.col("last_reported_at_date")).alias("last_reported_at_date_month"),
    )
    .orderBy(F.col("median_reported_at_month").desc(), "last_reported_at_date_month")
)
