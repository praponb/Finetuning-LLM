from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_status = Window.partitionBy("status")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_approved_amoun_2 = Window.partitionBy("claim_type").orderBy(F.col("approved_amount_filled").desc())
w_all_by_claim_date_cume_dist = Window.orderBy(F.col("claim_date_cume_dist").desc(), "id")
w_claim_type_by_approved_amoun_3 = Window.partitionBy("claim_type").orderBy(F.col("approved_amount_filled").desc(), "id")

filtered = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.col("claim_type") == "delay"))
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
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.max("approved_amount").over(w_status).alias("status_max_approved_amount"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "claim_date_cume_dist",
        "status_max_approved_amount",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.coalesce(F.col("claim_date_cume_dist"), F.lit(0.0)).alias("claim_date_cume_dist_filled"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.last("approved_amount_filled", ignorenulls=True).over(w_claim_type_by_approved_amoun).alias("last_known_approved_amount_filled"),
        F.round(F.percent_rank().over(w_claim_type_by_approved_amoun_2), 3).alias("approved_amount_filled_percent_rank"),
        F.lag("claim_date_cume_dist_filled", 2).over(w_all_by_claim_date_cume_dist).alias("prev2_claim_date_cume_dist_filled"),
        F.dense_rank().over(w_claim_type_by_approved_amoun_3).alias("approved_amount_filled_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "status_max_approved_amount",
        "approved_amount",
        "approved_amount_filled",
        "prev2_claim_date_cume_dist_filled",
    )
    .unpivot(
        ids=["id"],
        values=["status_max_approved_amount", "approved_amount", "approved_amount_filled", "prev2_claim_date_cume_dist_filled"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
