from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_approved_amount = Window.partitionBy("adjuster_id", "approved_amount_tier")
w_all_by_claim_date_prior_6 = Window.orderBy("claim_date", "id").rowsBetween(-6, -1)
w_all_by_claim_date = Window.orderBy("claim_date", "id")

base = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-06-20").cast("date")) & (F.col("claim_type").isin("medical", "fire")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.when(F.col("approved_amount") >= 15477.7, "large").when(F.col("approved_amount") >= 0.0, "medium").otherwise("small").alias("approved_amount_tier"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id_approved_amount).alias("rows_in_adjuster_id_approved_amount_tier"),
        F.max("claimed_amount").over(w_all_by_claim_date_prior_6).alias("claimed_amount_max_6_prior"),
        F.ntile(3).over(w_all_by_claim_date).alias("claim_date_tercile"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_approved_amount), 2).alias("claimed_amount_vs_adjuster_id_approved_amount_ti"),
    )
)

result = (
    ranked
    .select(
        "id",
        "claimed_amount_vs_adjuster_id_approved_amount_ti",
        "claimed_amount",
        "claimed_amount_max_6_prior",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_vs_adjuster_id_approved_amount_ti", "claimed_amount", "claimed_amount_max_6_prior"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
