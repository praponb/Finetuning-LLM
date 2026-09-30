from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_claim_type = Window.partitionBy(F.month("claim_date"), "claim_type").orderBy("claim_date", "id")
w_reported_at_month_by_reporte = Window.partitionBy(F.month("reported_at")).orderBy("reported_at", "id")
w_reported_at_month_by_reporte_2 = Window.partitionBy(F.month("reported_at")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_claim_type_2 = Window.partitionBy(F.month("claim_date"), "claim_type").orderBy("claim_date")

cleaned_claims = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(9, 14))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_details_policy = (
    cleaned_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.status") != "LAPSED")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        F.col("p.status").alias("policy_status"),
    )
)

metrics = (
    with_details_policy
    .select(
        "*",
        F.lag("claim_date", 2).over(w_claim_date_month_claim_type).alias("prev2_claim_date"),
        F.round(F.cume_dist().over(w_reported_at_month_by_reporte), 3).alias("reported_at_cume_dist"),
        F.first("reported_at", ignorenulls=True).over(w_reported_at_month_by_reporte_2).alias("first_known_reported_at"),
        F.rank().over(w_claim_date_month_claim_type_2).alias("claim_date_rank"),
    )
)

first_rows = metrics.filter(F.col("claim_date_rank") <= 2)

result = (
    first_rows
    .groupingSets(
        [["policy_id", "claim_date_rank"], ["policy_id"], ["claim_date_rank"]],
        "policy_id", "claim_date_rank",
    )
    .agg(
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(F.sum("claim_date_rank") / F.nullif(F.sum("reported_at_cume_dist"), F.lit(0)), 3).alias("claim_date_rank_to_reported_at_cume_dist_ratio"),
        F.min("claim_date_rank").alias("min_claim_date_rank"),
    )
)
