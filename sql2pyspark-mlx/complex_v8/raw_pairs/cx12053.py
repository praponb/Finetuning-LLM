from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount_adj = Window.orderBy("approved_amount_adj", "id")
w_status = Window.partitionBy("status")
w_claim_type_adjuster_team_by = Window.partitionBy("claim_type", "adjuster_team").orderBy("reported_at", "id")
w_claim_type_status_by_approve = Window.partitionBy("claim_type", "status").orderBy(F.col("approved_amount_adj").desc(), "id")
w_reported_at_month_adjuster_l = Window.partitionBy(F.date_trunc("month", "reported_at"), "adjuster_level")
w_all_by_approved_amount_adj_m = Window.orderBy("approved_amount_adj", "id").rowsBetween(-1, Window.currentRow)

recent = (
    df_claims
    .filter(F.col("claim_type") == "medical")
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
        F.round(F.col("approved_amount") * 0.9, 2).alias("approved_amount_adj"),
    )
)

joined = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(~F.col("a.team").isin("Property", "Motor"))
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.lead("reported_at").over(w_all_by_approved_amount_adj).alias("next_reported_at"),
        F.min("approved_amount_adj").over(w_status).alias("status_min_approved_amount_adj"),
        F.lag("claim_date", 2).over(w_all_by_approved_amount_adj).alias("prev2_claim_date"),
        F.row_number().over(w_claim_type_adjuster_team_by).alias("rn"),
        F.lag("claimed_amount").over(w_claim_type_status_by_approve).alias("prev_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_reported_at_month_adjuster_l)).alias("distinct_claim_type_in_reported_at_month_adjuste"),
        F.dense_rank().over(w_claim_type_status_by_approve).alias("approved_amount_adj_dense_rank"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_status), F.lit(0)), 4).alias("adjuster_level_share_of_status"),
        F.round(F.avg("approved_amount_adj").over(w_all_by_approved_amount_adj_m), 2).alias("approved_amount_adj_avg_2"),
    )
)

first_rows = sequenced.filter(F.col("rn") <= 5)

result = (
    first_rows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        "approved_amount_adj",
        "adjuster_name",
        "adjuster_team",
        "adjuster_level",
        "next_reported_at",
        "status_min_approved_amount_adj",
        "rn",
        "prev_claimed_amount",
        F.date_add(F.col("claim_date"), 7).alias("claim_date_plus_7d"),
    )
    .orderBy(F.col("claim_date").desc(), "id")
    .limit(10)
)
