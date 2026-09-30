from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_approved_amount = Window.orderBy(F.col("prior_approved_amount_60d").desc())
w_claim_date_year = Window.partitionBy(F.year("claim_date"))
w_all_by_prior_approved_amount_2 = Window.orderBy(F.col("prior_approved_amount_60d").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

annotated = (
    matched
    .filter(F.col("policy_id") >= 11)
    .select(
        "id",
        "claim_date",
        "prior_approved_amount_60d",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_prior_approved_amount), 3).alias("prior_approved_amount_60d_cume_dist"),
        F.round(F.col("prior_approved_amount_60d") - F.avg("prior_approved_amount_60d").over(w_claim_date_year), 2).alias("prior_approved_amount_60d_vs_claim_date_year_avg"),
        F.min("prior_approved_amount_60d").over(w_claim_date_year).alias("claim_date_year_min_prior_approved_amount_60d"),
        F.nth_value("claim_date", 2).over(w_all_by_prior_approved_amount_2).alias("second_claim_date"),
        F.round(F.col("prior_approved_amount_60d") / F.nullif(F.sum("prior_approved_amount_60d").over(w_claim_date_year), F.lit(0)), 4).alias("prior_approved_amount_60d_share_of_claim_date_ye"),
    )
)

result = (
    metrics
    .filter(F.col("second_claim_date").between(F.lit("2024-02-23").cast("date"), F.lit("2025-08-10").cast("date")))
    .select(
        "id",
        "claim_date",
        "claim_date_label",
        "prior_approved_amount_60d_cume_dist",
        "prior_approved_amount_60d_vs_claim_date_year_avg",
        "second_claim_date",
        "prior_approved_amount_60d_share_of_claim_date_ye",
        F.datediff(F.col("claim_date"), F.col("second_claim_date")).alias("days_second_claim_date_to_claim_date"),
    )
    .orderBy(
        F.col("prior_approved_amount_60d_cume_dist").desc_nulls_first(),
        F.col("second_claim_date").desc_nulls_first(),
    )
)
