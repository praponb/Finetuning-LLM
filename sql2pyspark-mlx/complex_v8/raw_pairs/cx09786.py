from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_month_by_claim_d = Window.partitionBy(F.date_trunc("month", "reported_at")).orderBy("claim_date")
w_claim_date_month_by_claim_da = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("claim_date").desc_nulls_first(), "id")
w_claim_type = Window.partitionBy("claim_type")
w_status_adjuster_id_by_report = Window.partitionBy("status", "adjuster_id").orderBy("reported_at")
w_status_adjuster_id_by_report_2 = Window.partitionBy("status", "adjuster_id").orderBy("reported_at", "id")
w_status_status_len = Window.partitionBy("status", "status_len")
w_status_len_by_claim_date = Window.partitionBy("status_len").orderBy("claim_date", "id")

base_claims = (
    df_claims
    .filter((F.col("status") != "IN_REVIEW") & (F.month(F.col("reported_at")).isin(1, 3)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

screened = (
    base_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

without_matches = (
    screened
    .crossJoin(screened.agg(F.max("status_len").alias("overall_max_status_len")))
    .filter(F.col("status_len") < F.col("overall_max_status_len"))
    .drop("overall_max_status_len")
)

windowed = (
    without_matches
    .select(
        "*",
        F.round(F.cume_dist().over(w_reported_at_month_by_claim_d), 3).alias("claim_date_cume_dist"),
        F.round(F.cume_dist().over(w_claim_date_month_by_claim_da), 3).alias("claim_date_cume_dist_2"),
        F.round(F.col("status_len") - F.avg("status_len").over(w_claim_type), 2).alias("status_len_vs_claim_type_avg"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.dense_rank().over(w_status_adjuster_id_by_report).alias("reported_at_dense_rank"),
        F.round(F.cume_dist().over(w_status_adjuster_id_by_report), 3).alias("reported_at_cume_dist"),
        F.lead("status_len_vs_claim_type_avg", 3).over(w_status_adjuster_id_by_report_2).alias("next3_status_len_vs_claim_type_avg"),
        F.count(F.lit(1)).over(w_status_status_len).alias("rows_in_status_status_len"),
        F.lag("reported_at_hour", 1, 0).over(w_status_len_by_claim_date).alias("prev_reported_at_hour"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at_hour",
        "claim_date_cume_dist",
        "claim_date_cume_dist_2",
        "status_len_vs_claim_type_avg",
        "reported_at_dense_rank",
        "next3_status_len_vs_claim_type_avg",
        "rows_in_status_status_len",
        "prev_reported_at_hour",
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

result = (
    enriched
    .select("id", "claimed_amount", "claim_date_cume_dist_2")
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "claim_date_cume_dist_2"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
