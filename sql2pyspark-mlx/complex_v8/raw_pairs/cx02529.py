from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_quarter_to_date = Window.partitionBy(F.to_date("claim_date_quarter"))
w_status_or_unknown_claim_date = Window.partitionBy("status_or_unknown", F.date_trunc("month", "claim_date")).orderBy(F.col("claimed_amount").desc(), "id")

filtered = (
    df_claims
    .filter((F.col("claimed_amount").between(10673.3, 46461.4)) | (~F.col("claim_type").isin("medical", "flood")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)

labeled = (
    filtered
    .filter(F.col("status_or_unknown").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "status_or_unknown",
        "claim_date_quarter",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

windowed = (
    labeled
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "claim_date_quarter",
        "claim_type_slug",
        "approved_amount_bucket",
        F.sum("claimed_amount").over(w_claim_date_quarter_to_date).alias("claim_date_quarter_to_date_sum_claimed_amount"),
        F.ntile(10).over(w_status_or_unknown_claim_date).alias("claimed_amount_decile"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_date_quarter_to_date), F.lit(0)), 4).alias("claimed_amount_share_of_claim_date_quarter_to_da"),
        F.count(F.lit(1)).over(w_claim_date_quarter_to_date).alias("rows_in_claim_date_quarter_to_date"),
    )
)

result = (
    windowed
    .groupBy("claimed_amount_decile")
    .pivot("claim_type", ["theft", "medical", "delay", "flood"])
    .agg(F.max("rows_in_claim_date_quarter_to_date"))
)
