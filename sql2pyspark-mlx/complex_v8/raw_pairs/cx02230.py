from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_by_start_da = Window.partitionBy(F.month("start_date")).orderBy(F.col("start_date").desc(), "id")
w_start_date_month_by_start_da_2 = Window.partitionBy(F.month("start_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, -1)
w_start_date_month_by_start_da_3 = Window.partitionBy(F.month("start_date")).orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_policys = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
        F.month(F.col("end_date")).alias("end_date_month"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

ranked = (
    recent_policys
    .select(
        "*",
        F.lead("premium").over(w_start_date_month_by_start_da).alias("next_premium"),
        F.round(F.avg("premium").over(w_start_date_month_by_start_da_2), 2).alias("premium_avg_6_prior"),
        F.nth_value("premium", 2).over(w_start_date_month_by_start_da_3).alias("second_premium"),
        F.lead("premium", 3).over(w_start_date_month_by_start_da).alias("next3_premium"),
    )
)

line_level_claims = (
    ranked.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        "s.end_date_month",
        "s.next_premium",
        "s.premium_avg_6_prior",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

with_details_claim = (
    line_level_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        "s.end_date_month",
        "s.next_premium",
        "s.premium_avg_6_prior",
        "s.claim_id",
        "s.claim_reported_at",
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

enriched_claim = (
    with_details_claim.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        "s.end_date_month",
        "s.next_premium",
        "s.premium_avg_6_prior",
        "s.claim_id",
        "s.claim_reported_at",
        "s.claim_reported_at_2",
        "c.claim_date",
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

joined = (
    enriched_claim.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.claim_policy_id"), "inner")
    .filter(F.col("p.product") != "HOME")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        "s.end_date_month",
        "s.next_premium",
        "s.premium_avg_6_prior",
        "s.claim_id",
        "s.claim_policy_id",
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

result = (
    joined
    .select(
        "id",
        "claim_id",
        "premium_avg_6_prior",
        "next_premium",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["premium_avg_6_prior", "next_premium"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
