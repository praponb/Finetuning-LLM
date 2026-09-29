from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level_policy_id_by = Window.partitionBy("adjuster_level", "policy_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_quarter_year_by_c = Window.partitionBy(F.year("claim_date_quarter")).orderBy("claim_date_quarter", "id")
w_adjuster_level_policy_id_by_2 = Window.partitionBy("adjuster_level", "policy_id").orderBy(F.col("claimed_amount").desc_nulls_first())
w_adjuster_level_2_claim_date = Window.partitionBy("adjuster_level_2", F.date_trunc("month", "claim_date"))
w_dedup = Window.partitionBy("adjuster_team").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

scoped = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(9, 11)) | (F.col("status") == "OPEN"))
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
        F.add_months(F.col("claim_date"), 1).alias("claim_date_plus_1m"),
        F.when(F.col("status").isin("REJECTED", "IN_REVIEW"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)

with_details_adjuster = (
    scoped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "a.adjuster_name", F.col("a.level").alias("adjuster_level"))
)

windowed = (
    with_details_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "claim_date_plus_1m",
        "status_group",
        "claim_date_quarter",
        "adjuster_level",
        F.nth_value("approved_amount", 2).over(w_adjuster_level_policy_id_by).alias("second_approved_amount"),
        F.lag("reported_at").over(w_claim_date_quarter_year_by_c).alias("prev_reported_at"),
        F.round(F.cume_dist().over(w_adjuster_level_policy_id_by_2), 3).alias("claimed_amount_cume_dist"),
    )
)

with_details = (
    windowed.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") <= 3), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.claim_date_plus_1m",
        "s.status_group",
        "s.claim_date_quarter",
        "s.adjuster_level",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level_2"),
        F.col("a.team").alias("adjuster_team"),
    )
)

sequenced = (
    with_details
    .withColumn("rows_in_adjuster_level_2_claim_date_month", F.count(F.lit(1)).over(w_adjuster_level_2_claim_date))
)

latest = (
    sequenced
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_adjuster = (
    latest.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team_2"),
        F.col("a.level").alias("adjuster_level_3"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    enriched_adjuster
    .filter(F.col("policy_id") <= 4)
    .select(
        "id",
        "claim_date",
        "claim_date_plus_1m",
        "status_group",
        "claim_date_quarter",
        "adjuster_level",
        "adjuster_name",
        "adjuster_level_2",
        "rows_in_adjuster_level_2_claim_date_month",
        "adjuster_team_2",
        "adjuster_level_3",
        "adjuster_adjuster_name",
        "policy_start_date",
        F.abs(F.col("adjuster_level_2") - 2).alias("adjuster_level_2_distance"),
        F.coalesce(F.col("adjuster_level"), F.lit(0)).alias("adjuster_level_filled"),
    )
    .orderBy(F.col("rows_in_adjuster_level_2_claim_date_month").asc_nulls_last())
)
