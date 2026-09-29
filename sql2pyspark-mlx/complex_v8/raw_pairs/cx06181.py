from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_all_by_claim_date_running = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date = Window.orderBy("claim_date", "id")

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.round(F.col("approved_amount") * 0.9, 2).alias("approved_amount_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

joined = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.reported_at",
        "s.approved_amount_adj",
        "s.claim_type_slug",
        "a.adjuster_name",
    )
)

ranked = (
    joined
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "approved_amount_adj",
        F.last("reported_at", ignorenulls=True).over(w_adjuster_id_by_approved_amou).alias("last_known_reported_at"),
        F.sum("approved_amount").over(w_policy_id).alias("policy_id_sum_approved_amount"),
        (F.col("approved_amount_adj") - F.lag("approved_amount_adj").over(w_adjuster_id_by_approved_amou_2)).alias("approved_amount_adj_change"),
        F.count(F.lit(1)).over(w_reported_at_to_date).alias("rows_in_reported_at_to_date"),
        F.ntile(10).over(w_adjuster_id_by_approved_amou_2).alias("approved_amount_decile"),
        F.max("approved_amount_adj").over(w_all_by_claim_date_running).alias("max_approved_amount_adj_so_far"),
        F.round(F.col("approved_amount_adj") - F.avg("approved_amount_adj").over(w_policy_id), 2).alias("approved_amount_adj_vs_policy_id_avg"),
        F.round(F.avg("approved_amount").over(w_reported_at_to_date), 2).alias("reported_at_to_date_avg_approved_amount"),
        F.ntile(4).over(w_all_by_claim_date).alias("claim_date_quartile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "claim_date",
        "reported_at",
        "approved_amount_adj",
        "last_known_reported_at",
        "policy_id_sum_approved_amount",
        "rows_in_reported_at_to_date",
        "approved_amount_decile",
        "max_approved_amount_adj_so_far",
        "approved_amount_adj_vs_policy_id_avg",
        "claim_date_quartile",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
    )
    .orderBy("rows_in_reported_at_to_date", F.col("approved_amount_decile").desc())
)
