from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_product = Window.partitionBy("adjuster_id", "policy_product")
w_policy_product_adjuster_team = Window.partitionBy("policy_product", "adjuster_team").orderBy("reported_at", "id")
w_policy_product_policy_id = Window.partitionBy("policy_product", "policy_id")
w_policy_product_adjuster_team_2 = Window.partitionBy("policy_product", "adjuster_team").orderBy("reported_at")
w_policy_product_adjuster_team_3 = Window.partitionBy("policy_product", "adjuster_team").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_level_adjuster_team = Window.partitionBy("adjuster_level", "adjuster_team").orderBy("approved_amount")
w_adjuster_id_policy_product_b = Window.partitionBy("adjuster_id", "policy_product").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_claims = (
    df_claims
    .filter(F.col("claimed_amount") <= 29359.4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

enriched = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.product").alias("policy_product"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_policy_product), 2).alias("approved_amount_vs_adjuster_id_policy_product_av"),
        F.row_number().over(w_policy_product_adjuster_team).alias("reported_at_seq"),
        F.sum("adjuster_level").over(w_adjuster_id_policy_product).alias("adjuster_id_policy_product_sum_adjuster_level"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_product_policy_id), 2).alias("claimed_amount_vs_policy_product_policy_id_avg"),
        F.count(F.lit(1)).over(w_policy_product_policy_id).alias("rows_in_policy_product_policy_id"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_adjuster_id_policy_product), F.lit(0)), 4).alias("adjuster_level_share_of_adjuster_id_policy_produ"),
        F.rank().over(w_policy_product_adjuster_team_2).alias("reported_at_rank"),
        F.last("claimed_amount").over(w_policy_product_adjuster_team_3).alias("last_claimed_amount"),
        F.size(F.collect_set("adjuster_team").over(w_policy_product_policy_id)).alias("distinct_adjuster_team_in_policy_product_policy_"),
        F.size(F.collect_set("adjuster_id").over(w_policy_product_policy_id)).alias("distinct_adjuster_id_in_policy_product_policy_id"),
        F.round(F.percent_rank().over(w_adjuster_level_adjuster_team), 3).alias("approved_amount_percent_rank"),
        F.first("adjuster_level", ignorenulls=True).over(w_adjuster_id_policy_product_b).alias("first_known_adjuster_level"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("reported_at_seq") <= 5)
    .drop("reported_at_seq")
)

result = (
    first_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "adjuster_team",
        "adjuster_level",
        "approved_amount_vs_adjuster_id_policy_product_av",
        "adjuster_id_policy_product_sum_adjuster_level",
        "claimed_amount_vs_policy_product_policy_id_avg",
        "rows_in_policy_product_policy_id",
        "reported_at_rank",
        "last_claimed_amount",
        "distinct_adjuster_team_in_policy_product_policy_",
        "approved_amount_percent_rank",
        "first_known_adjuster_level",
        F.round(F.col("claimed_amount") / F.nullif(F.col("adjuster_level"), F.lit(0)), 3).alias("claimed_amount_per_adjuster_level"),
        F.coalesce(F.col("adjuster_level"), F.lit(0)).alias("adjuster_level_filled"),
    )
    .orderBy("claimed_amount_vs_policy_product_policy_id_avg", "id")
    .limit(5)
)
