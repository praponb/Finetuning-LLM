from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_claim_type_by_reported_at_ho = Window.partitionBy("claim_type").orderBy("reported_at_hour", "id")
w_all_by_reported_at_hour_ties = Window.orderBy("reported_at_hour")
w_claim_type_by_reported_at_ho_2 = Window.partitionBy("claim_type").orderBy("reported_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_reported_at_ho_3 = Window.partitionBy("claim_type").orderBy("reported_at_hour")
w_all_by_reported_at_hour = Window.orderBy("reported_at_hour", "id")
w_dedup = Window.partitionBy("claim_type", "status").orderBy(F.col("reported_at").desc(), F.col("id").desc())

recent_claims = (
    df_claims
    .filter(F.col("approved_amount").between(492.6, 21750.2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
)

tagged = (
    recent_claims
    .filter(F.col("claim_type_ref").isNotNull())
    .select(
        "*",
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "collision", 1)).alias("claim_claim_type_count"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

windowed = (
    with_counts
    .select(
        "*",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type_adjuster_id), 2).alias("approved_amount_vs_claim_type_adjuster_id_avg"),
        F.row_number().over(w_claim_type_by_reported_at_ho).alias("seq_no"),
        F.dense_rank().over(w_all_by_reported_at_hour_ties).alias("reported_at_hour_dense_rank"),
        F.last("approved_amount", ignorenulls=True).over(w_claim_type_by_reported_at_ho_2).alias("last_known_approved_amount"),
        F.rank().over(w_claim_type_by_reported_at_ho_3).alias("reported_at_hour_rank"),
        F.row_number().over(w_all_by_reported_at_hour).alias("seq_no_2"),
    )
)

unique_rows = (
    windowed
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "seq_no_2",
        "claim_claim_type_count",
        "seq_no",
        "reported_at_hour_rank",
    )
    .unpivot(
        ids=["id"],
        values=["seq_no_2", "claim_claim_type_count", "seq_no", "reported_at_hour_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .orderBy("id", "measure")
)
