from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_least_claimed_amount = Window.orderBy("least_claimed_amount_approved_amount", "id")
w_policy_id = Window.partitionBy("policy_id")
w_claim_type_len = Window.partitionBy("claim_type_len")

valid = (
    df_claims
    .filter(F.col("claimed_amount") >= 19609.8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

with_trends = (
    valid
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "claim_type_len",
        "least_claimed_amount_approved_amount",
        "greatest_claimed_amount_approved_amount",
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_least_claimed_amount)).alias("claimed_amount_change"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_least_claimed_amount)).alias("days_since_prev_claim_date"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.min("approved_amount").over(w_policy_id).alias("policy_id_min_approved_amount"),
    )
)

windowed = (
    with_trends
    .withColumn("claim_type_len_vs_claim_type_len_avg", F.round(F.col("claim_type_len") - F.avg("claim_type_len").over(w_claim_type_len), 2))
    .withColumn("least_claimed_amount_approved_amount_percent_ran", F.round(F.percent_rank().over(w_all_by_least_claimed_amount), 3))
)

summary = (
    windowed
    .filter(F.col("approved_amount") <= 23120.7)
    .groupBy("status", F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"))
    .agg(
        F.round(F.sum("least_claimed_amount_approved_amount_percent_ran") / F.nullif(F.sum("claim_type_len"), F.lit(0)), 3).alias("least_claimed_amount_approved_amount_percent_ran_2"),
        F.round(F.sum("greatest_claimed_amount_approved_amount") / F.nullif(F.sum("policy_id_min_approved_amount"), F.lit(0)), 3).alias("greatest_claimed_amount_approved_amount_to_polic"),
        F.max("claim_type_len_vs_claim_type_len_avg").alias("max_claim_type_len_vs_claim_type_len_avg"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

tagged = (
    summary
    .select(
        "status",
        "claim_date_ym",
        "max_claim_type_len_vs_claim_type_len_avg",
        "row_count",
        F.concat_ws("|", F.col("claim_date_ym"), F.col("status")).alias("claim_date_ym_status_label"),
    )
)

result = (
    tagged
    .select(
        "status",
        "claim_date_ym",
        "max_claim_type_len_vs_claim_type_len_avg",
        "row_count",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
    .orderBy(F.col("claim_date_ym").desc(), F.col("row_count").desc())
)
