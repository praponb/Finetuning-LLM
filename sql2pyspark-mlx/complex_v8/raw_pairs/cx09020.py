from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_claimed_amount_plus_a = Window.orderBy(F.col("claimed_amount_plus_approved_amount").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_claimed_amount_plus_a_2 = Window.orderBy(F.col("claimed_amount_plus_approved_amount").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.round(F.avg("approved_amount").over(w_all_by_claimed_amount_plus_a), 2).alias("approved_amount_avg_4"),
        F.max("claimed_amount").over(w_all_by_claimed_amount_plus_a_2).alias("claimed_amount_max_7"),
    )
)

result = (
    windowed
    .groupingSets(
        [["claim_type", "status"], ["claim_type"], ["status"], []],
        "claim_type", "status",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("claim_type") == "collision", F.col("claimed_amount_max_7")).otherwise(0)).alias("claimed_amount_max_7_claim_type_collision"),
    )
    .select(
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "row_count",
        "claimed_amount_max_7_claim_type_collision",
    )
)
