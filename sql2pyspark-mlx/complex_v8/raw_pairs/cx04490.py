from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_prior_rows_60d = Window.partitionBy("policy_id").orderBy("prior_rows_60d", "id").rowsBetween(-2, Window.currentRow)
w_prior_rows_60d = Window.partitionBy("prior_rows_60d")
w_policy_id_by_prior_rows_60d_2 = Window.partitionBy("policy_id").orderBy("prior_rows_60d", "id")

covered = (
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

windowed = (
    covered
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_60d",
        "prior_approved_amount_60d",
        F.round(F.avg("prior_approved_amount_60d").over(w_policy_id_by_prior_rows_60d), 2).alias("prior_approved_amount_60d_avg_3"),
        F.round(F.col("prior_rows_60d") - F.avg("prior_rows_60d").over(w_prior_rows_60d), 2).alias("prior_rows_60d_vs_prior_rows_60d_avg"),
        F.lag("prior_rows_60d", 2).over(w_policy_id_by_prior_rows_60d_2).alias("prev2_prior_rows_60d"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_60d",
        "prior_approved_amount_60d",
        "prior_rows_60d_vs_prior_rows_60d_avg",
        "prev2_prior_rows_60d",
        F.when(F.col("prev2_prior_rows_60d") >= 5, "large").when(F.col("prev2_prior_rows_60d") >= 2, "medium").otherwise("small").alias("prev2_prior_rows_60d_tier"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
    .orderBy(
        "prior_rows_60d",
        F.col("prev2_prior_rows_60d_tier").desc_nulls_first(),
        "id",
    )
    .limit(5)
)
