from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_reported_at_year = Window.partitionBy("policy_id", "reported_at_year")
w_adjuster_team_by_approved_am = Window.partitionBy("adjuster_team").orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_team_by_approved_am_2 = Window.partitionBy("adjuster_team").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-1, Window.currentRow)
w_dedup = Window.partitionBy("status").orderBy(F.col("approved_amount_sum_2").desc(), F.col("id").desc())
w_latest = Window.partitionBy("adjuster_team_group", "adjuster_id").orderBy(F.col("approved_amount_sum_2").desc(), F.col("id").desc())

base_claims = (
    df_claims
    .filter((F.col("policy_id") <= 2) & (F.col("adjuster_id") >= 5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.year(F.col("reported_at")).alias("reported_at_year"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

with_parent_adjuster = (
    base_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.upper(F.trim(F.col("a.adjuster_name"))).like("G%"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_year",
        "s.least_approved_amount_claimed_amount",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.premium").alias("policy_premium"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("approved_amount").alias("max_claim_approved_amount"))
)

with_stats = (
    with_parent_adjuster.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.max_claim_approved_amount")
)

with_windows = (
    with_stats
    .select(
        "*",
        F.min("reported_at_year").over(w_policy_id_reported_at_year).alias("policy_id_reported_at_year_min_reported_at_year"),
        F.size(F.collect_set("status").over(w_policy_id_reported_at_year)).alias("distinct_status_in_policy_id_reported_at_year"),
        F.round(F.cume_dist().over(w_adjuster_team_by_approved_am), 3).alias("approved_amount_cume_dist"),
        F.sum("approved_amount").over(w_adjuster_team_by_approved_am_2).alias("approved_amount_sum_2"),
    )
)

derived = (
    with_windows
    .filter(F.col("approved_amount_sum_2") > 30000.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        "adjuster_name",
        "adjuster_level",
        "max_claim_approved_amount",
        "policy_id_reported_at_year_min_reported_at_year",
        "approved_amount_cume_dist",
        "approved_amount_sum_2",
        F.when(F.col("adjuster_team").isin("Health", "Property"), "group_a").when(F.col("adjuster_team") == "Travel", "group_b").otherwise("other").alias("adjuster_team_group"),
    )
)

unique_rows = (
    derived
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows_2 = (
    unique_rows
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows_2
    .select(
        "id",
        "max_claim_approved_amount",
        "approved_amount_cume_dist",
        "approved_amount_sum_2",
    )
    .unpivot(
        ids=["id"],
        values=["max_claim_approved_amount", "approved_amount_cume_dist", "approved_amount_sum_2"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
