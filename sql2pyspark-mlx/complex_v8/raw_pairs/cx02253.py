from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_reported_at_month_by_reporte = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy(F.col("reported_at").desc(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_reported_at_month_by_reporte_2 = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(-3, -1)
w_reported_at_month_by_reporte_3 = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.when(F.col("status").isin("OPEN", "REJECTED"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status_group",
        F.lead("claim_date").over(w_all_by_reported_at).alias("next_claim_date"),
        F.ntile(4).over(w_reported_at_month_by_reporte).alias("reported_at_quartile"),
        F.round(F.avg("approved_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_approved_amount"),
        F.sum("claimed_amount").over(w_reported_at_month_by_reporte_2).alias("claimed_amount_sum_3_prior"),
        F.last("claim_date").over(w_reported_at_month_by_reporte_3).alias("last_claim_date"),
    )
)

result = (
    metrics
    .select(
        "id",
        "adjuster_id_avg_approved_amount",
        "claimed_amount",
        "claimed_amount_sum_3_prior",
    )
    .unpivot(
        ids=["id"],
        values=["adjuster_id_avg_approved_amount", "claimed_amount", "claimed_amount_sum_3_prior"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .orderBy("id", "metric_name")
)
