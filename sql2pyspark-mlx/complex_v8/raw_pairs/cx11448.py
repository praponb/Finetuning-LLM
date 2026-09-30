from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_reported_at_hour = Window.partitionBy("claim_type", "reported_at_hour_quarter")
w_claim_date_year_by_claim_dat = Window.partitionBy(F.year("claim_date")).orderBy("claim_date", "id")

scoped_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 34763.4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.when(F.col("claim_type").isin("medical", "theft"), "group_a").when(F.col("claim_type") == "flood", "group_b").otherwise("other").alias("claim_type_group"),
        F.date_trunc("hour", F.col("reported_at")).alias("reported_at_hour"),
    )
)

labeled = (
    scoped_claims
    .select(
        "*",
        F.quarter(F.col("reported_at_hour")).alias("reported_at_hour_quarter"),
    )
)

sequenced = (
    labeled
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type_reported_at_hour).alias("rows_in_claim_type_reported_at_hour_quarter"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_date_year_by_claim_dat)).alias("days_since_prev_claim_date"),
        F.size(F.collect_set("policy_id").over(w_claim_type_reported_at_hour)).alias("distinct_policy_id_in_claim_type_reported_at_hou"),
        F.max("claimed_amount").over(w_claim_type_reported_at_hour).alias("claim_type_reported_at_hour_quarter_max_claimed_"),
        F.round(F.col("reported_at_hour_quarter") / F.nullif(F.sum("reported_at_hour_quarter").over(w_claim_type_reported_at_hour), F.lit(0)), 4).alias("reported_at_hour_quarter_share_of_claim_type_rep"),
    )
)

result = (
    sequenced
    .filter(F.col("rows_in_claim_type_reported_at_hour_quarter").between(5, 14))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "claim_type_group",
        "reported_at_hour",
        "reported_at_hour_quarter",
        "rows_in_claim_type_reported_at_hour_quarter",
        "days_since_prev_claim_date",
        "claim_type_reported_at_hour_quarter_max_claimed_",
        "reported_at_hour_quarter_share_of_claim_type_rep",
    )
    .orderBy("days_since_prev_claim_date", "id")
    .limit(5)
)
