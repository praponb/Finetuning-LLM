from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_status = Window.partitionBy("policy_id", "status")
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_policy_product_by = Window.partitionBy("policy_id", "policy_product").orderBy("reported_at", "id")
w_policy_id_policy_product_by_2 = Window.partitionBy("policy_id", "policy_product").orderBy("reported_at")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc(), "id")
w_reported_at_month = Window.partitionBy(F.date_trunc("month", "reported_at"))
w_adjuster_id_by_reported_at = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), "id")

prepared_claims = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2024-10-22").cast("date")) & (F.col("claimed_amount") > 10518.4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

with_details_policy = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.year(F.col("p.end_date")) == 2026), "left")
    .select("s.*", F.col("p.product").alias("policy_product"))
)

ranked = (
    with_details_policy
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "reported_at",
        "status_prefix",
        "policy_product",
        F.max("claimed_amount").over(w_policy_id_status).alias("policy_id_status_max_claimed_amount"),
        F.first("claimed_amount", ignorenulls=True).over(w_adjuster_id_by_claimed_amoun).alias("first_known_claimed_amount"),
        F.lag("reported_at").over(w_policy_id_policy_product_by).alias("prev_reported_at"),
        F.round(F.percent_rank().over(w_policy_id_policy_product_by_2), 3).alias("reported_at_percent_rank"),
        F.row_number().over(w_all_by_claimed_amount).alias("rn"),
        F.size(F.collect_set("policy_id").over(w_reported_at_month)).alias("distinct_policy_id_in_reported_at_month"),
        F.max("claimed_amount").over(w_reported_at_month).alias("reported_at_month_max_claimed_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_claimed_amount)).alias("claimed_amount_change"),
        F.lead("claimed_amount", 2, 0).over(w_adjuster_id_by_reported_at).alias("next2_claimed_amount"),
        F.dense_rank().over(w_policy_id_policy_product_by_2).alias("reported_at_dense_rank"),
        F.lag("claimed_amount").over(w_all_by_claimed_amount).alias("prev_claimed_amount"),
    )
)

leaders = ranked.filter(F.col("reported_at_dense_rank") == 1)

result = (
    leaders
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "policy_product",
        "first_known_claimed_amount",
        "reported_at_percent_rank",
        "rn",
        "distinct_policy_id_in_reported_at_month",
        "reported_at_month_max_claimed_amount",
        "claimed_amount_change",
        "reported_at_dense_rank",
        "prev_claimed_amount",
    )
    .orderBy(F.col("prev_claimed_amount").asc(), "id")
    .limit(20)
)
