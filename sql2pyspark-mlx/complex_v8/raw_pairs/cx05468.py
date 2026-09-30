from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").asc_nulls_last())
w_claim_type = Window.partitionBy("claim_type")
w_claim_type_by_claim_date = Window.partitionBy("claim_type").orderBy(F.col("claim_date").desc(), "id")

prepared_claims = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(8, 14))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
    )
)

sequenced = (
    prepared_claims
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_cume_dist"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type)).alias("distinct_adjuster_id_in_claim_type"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_by_claim_date)).alias("days_since_prev_claim_date"),
    )
)

result = (
    sequenced
    .groupBy("distinct_adjuster_id_in_claim_type")
    .pivot("claim_type", ["delay", "theft", "fire"])
    .agg(F.max("approved_amount_vs_claim_type_avg").alias("max_v"), F.sum("approved_amount_vs_claim_type_avg").alias("sum_v"))
)
