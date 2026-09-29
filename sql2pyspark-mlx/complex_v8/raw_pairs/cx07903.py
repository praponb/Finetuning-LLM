from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc())
w_policy_product_by_adjuster_l = Window.partitionBy("policy_product").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-2, Window.currentRow)
w_policy_product_adjuster_id = Window.partitionBy("policy_product", "adjuster_id")
w_policy_product_by_adjuster_l_2 = Window.partitionBy("policy_product").orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_level_policy_produc = Window.partitionBy("adjuster_level", "policy_product").orderBy(F.col("claimed_amount_plus_approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_by_claim_date = Window.partitionBy("adjuster_level").orderBy("claim_date", "id")
w_adjuster_team = Window.partitionBy("adjuster_team")

recent = (
    df_claims
    .filter(F.col("policy_id") >= 24)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
        F.concat_ws("|", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") < 3), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claimed_amount_plus_approved_amount",
        "s.claim_type_status_label",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.rank().over(w_policy_id_by_approved_amount).alias("approved_amount_rank"),
        F.round(F.avg("claimed_amount").over(w_policy_product_by_adjuster_l), 2).alias("claimed_amount_avg_3"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_product_adjuster_id), 2).alias("approved_amount_vs_policy_product_adjuster_id_av"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_product_by_adjuster_l_2)).alias("claimed_amount_change"),
        F.ntile(10).over(w_policy_product_by_adjuster_l_2).alias("adjuster_level_decile"),
        F.first("policy_premium").over(w_adjuster_level_policy_produc).alias("first_policy_premium"),
        F.ntile(5).over(w_adjuster_level_by_claim_date).alias("claim_date_quintile"),
        F.min("adjuster_level").over(w_adjuster_team).alias("adjuster_team_min_adjuster_level"),
        F.ntile(4).over(w_adjuster_level_by_claim_date).alias("claim_date_quartile"),
        F.sum("adjuster_level").over(w_adjuster_team).alias("adjuster_team_sum_adjuster_level"),
        F.count(F.lit(1)).over(w_policy_product_adjuster_id).alias("rows_in_policy_product_adjuster_id"),
        F.count(F.lit(1)).over(w_adjuster_team).alias("rows_in_adjuster_team"),
    )
)

leaders = (
    sequenced
    .filter((F.col("approved_amount_rank") <= 5) & (F.col("claimed_amount_plus_approved_amount") < 60000.0))
    .drop("approved_amount_rank")
)

result = (
    leaders
    .filter(F.col("rows_in_policy_product_adjuster_id") >= 8)
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "adjuster_name",
        "policy_premium",
        "policy_product",
        "claimed_amount_avg_3",
        "approved_amount_vs_policy_product_adjuster_id_av",
        "claimed_amount_change",
        "first_policy_premium",
        "adjuster_team_min_adjuster_level",
        "claim_date_quartile",
        "rows_in_adjuster_team",
        (F.floor(F.col("claimed_amount_change") / 50000) * 50000).alias("claimed_amount_change_bucket"),
    )
    .orderBy("claimed_amount_change")
)
