from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type = Window.partitionBy("status", "claim_type")
w_policy_id_status_by_reported = Window.partitionBy("policy_id", "status").orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_claims = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

sequenced = (
    base_claims
    .select(
        "id",
        "reported_at",
        "claim_type_clean",
        "status_claim_type_label",
        "approved_amount_plus_claimed_amount",
        F.count(F.lit(1)).over(w_status_claim_type).alias("rows_in_status_claim_type"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_claim_type), 2).alias("approved_amount_vs_status_claim_type_avg"),
        F.last("approved_amount", ignorenulls=True).over(w_policy_id_status_by_reported).alias("last_known_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_status_claim_type"),
        F.max("approved_amount").over(w_status_claim_type).alias("status_claim_type_max_approved_amount"),
    )
)

tagged = sequenced.withColumn("claim_type_clean_len", F.length(F.trim(F.col("claim_type_clean"))))

result = (
    tagged
    .select("id", "rows_in_status_claim_type", "claim_type_clean_len")
    .unpivot(
        ids=["id"],
        values=["rows_in_status_claim_type", "claim_type_clean_len"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
