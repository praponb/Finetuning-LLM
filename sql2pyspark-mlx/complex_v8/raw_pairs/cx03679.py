from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_start_date_ties = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc())
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id")
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_next3_start_date_r = Window.partitionBy("status").orderBy("next3_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_start_date_rank_by_st = Window.partitionBy("status", "start_date_rank").orderBy("start_date")
w_claim_status = Window.partitionBy("claim_status")
w_claim_status_by_claim_approv = Window.partitionBy("claim_status").orderBy("claim_approved_amount", "id", "claim_id")

filtered = (
    df_policies
    .filter((F.col("product").isin("AUTO", "HEALTH")) & (F.col("end_date") < F.lit("2026-12-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_holder_id_by_start_date_ties).alias("start_date_dense_rank"),
        F.rank().over(w_holder_id_by_start_date_ties).alias("start_date_rank"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_by_start_date)).alias("days_since_prev_start_date"),
        F.lead("start_date", 3).over(w_holder_id_by_start_date).alias("next3_start_date"),
        F.first("start_date").over(w_holder_id_by_start_date_full).alias("first_start_date"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "holder_id",
        "product",
        "start_date_dense_rank",
        "start_date_rank",
        "days_since_prev_start_date",
        "next3_start_date",
        "first_start_date",
        F.sum("start_date_dense_rank").over(w_status_by_next3_start_date_r).alias("running_start_date_dense_rank"),
        F.round(F.percent_rank().over(w_status_start_date_rank_by_st), 3).alias("start_date_percent_rank"),
    )
)

exploded_detail_claims = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isNotNull()), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date_dense_rank",
        "s.start_date_rank",
        "s.next3_start_date",
        "s.first_start_date",
        "s.running_start_date_dense_rank",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

with_trends = (
    exploded_detail_claims
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_status).alias("rows_in_claim_status"),
        (F.col("start_date_rank") - F.lag("start_date_rank").over(w_claim_status_by_claim_approv)).alias("start_date_rank_change"),
        F.sum("start_date_rank").over(w_claim_status).alias("claim_status_sum_start_date_rank"),
        F.max("start_date_dense_rank").over(w_claim_status).alias("claim_status_max_start_date_dense_rank"),
    )
)

result = (
    with_trends
    .select("start_date_dense_rank", "first_start_date")
    .distinct()
    .orderBy("first_start_date")
)
