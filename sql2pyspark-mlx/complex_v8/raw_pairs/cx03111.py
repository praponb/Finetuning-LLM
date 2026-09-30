from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_type_or_un = Window.partitionBy("adjuster_id", "claim_type_or_unknown")
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id")
w_claim_type_or_unknown_adjust = Window.partitionBy("claim_type_or_unknown", "adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_claim_type_by_reported_at_ti = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc())

base = (
    df_claims
    .filter((F.col("claim_type") != "collision") | (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_adjuster_id_claim_type_or_un)).alias("distinct_claim_type_in_adjuster_id_claim_type_or"),
        F.row_number().over(w_claim_type_by_reported_at).alias("reported_at_seq"),
        F.round(F.avg("claimed_amount").over(w_claim_type_or_unknown_adjust), 2).alias("claimed_amount_avg_4"),
        F.round(F.percent_rank().over(w_claim_type_by_reported_at_ti), 3).alias("reported_at_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "claimed_amount",
        "reported_at_percent_rank",
        "claimed_amount_avg_4",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "reported_at_percent_rank", "claimed_amount_avg_4"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
