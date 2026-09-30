from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_by_total_approved_amount = Window.partitionBy("cnt").orderBy("total_approved_amount", "claim_type", "reported_at_month", "policy_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cnt_claim_type_by_max_approv = Window.partitionBy("cnt", "claim_type").orderBy("max_approved_amount_plus_cnt")
w_cnt_by_total_approved_amount_2 = Window.partitionBy("cnt").orderBy("total_approved_amount", "claim_type", "reported_at_month", "policy_id")
w_cnt_claim_type_by_max_approv_2 = Window.partitionBy("cnt", "claim_type").orderBy("max_approved_amount_plus_cnt", "claim_type", "reported_at_month", "policy_id")

scoped_claims = df_claims.filter(F.col("adjuster_id") <= 5)

tagged = (
    scoped_claims
    .filter(F.col("approved_amount") <= 23487.5)
    .select(
        "*",
        F.concat_ws("|", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

per_group = (
    tagged
    .filter(F.col("claimed_amount") < 38909.4)
    .groupBy(
        "claim_type",
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        "policy_id",
    )
    .agg(
        F.max("approved_amount").alias("max_approved_amount"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.count("*").alias("cnt"),
    )
)

labeled = (
    per_group
    .filter(F.col("reported_at_month") < F.lit("2025-02-09 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.least(F.col("cnt"), F.col("max_approved_amount")).alias("least_cnt_max_approved_amount"),
        (F.col("max_approved_amount") + F.col("cnt")).alias("max_approved_amount_plus_cnt"),
        F.date_format(F.col("reported_at_month"), "yyyy-MM-dd").alias("reported_at_month_label"),
    )
)

sequenced = (
    labeled
    .select(
        "claim_type",
        "reported_at_month",
        "policy_id",
        "max_approved_amount",
        "total_approved_amount",
        "claim_type_list",
        "cnt",
        "least_cnt_max_approved_amount",
        "reported_at_month_label",
        F.nth_value("max_approved_amount_plus_cnt", 2).over(w_cnt_by_total_approved_amount).alias("second_max_approved_amount_plus_cnt"),
        F.rank().over(w_cnt_claim_type_by_max_approv).alias("max_approved_amount_plus_cnt_rank"),
        F.lead("total_approved_amount").over(w_cnt_by_total_approved_amount_2).alias("next_total_approved_amount"),
        F.ntile(3).over(w_cnt_claim_type_by_max_approv_2).alias("max_approved_amount_plus_cnt_tercile"),
    )
)

result = (
    sequenced
    .select(
        "claim_type",
        "reported_at_month",
        "policy_id",
        "max_approved_amount",
        "total_approved_amount",
        "claim_type_list",
        "least_cnt_max_approved_amount",
        "reported_at_month_label",
        "second_max_approved_amount_plus_cnt",
        "max_approved_amount_plus_cnt_rank",
        "next_total_approved_amount",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at_month")).alias("days_since_reported_at_month"),
    )
    .orderBy(F.col("policy_id").desc())
)
