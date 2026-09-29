from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_product = Window.partitionBy("status", "product")
w_policy_id_by_premium_running = Window.partitionBy("policy_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_claim_date = Window.partitionBy("adjuster_id").orderBy("claim_date", "id")
w_adjuster_id_by_claim_date_ru = Window.partitionBy("adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_claim_date_ti = Window.partitionBy("adjuster_id").orderBy("claim_date")
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_claim_date_year_by_running_c = Window.partitionBy(F.year("claim_date")).orderBy(F.col("running_claimed_amount").desc())
w_claim_date_year_by_running_c_2 = Window.partitionBy(F.year("claim_date")).orderBy(F.col("running_claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

with_trends = (
    matched
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_status_product), 2).alias("premium_vs_status_product_avg"),
        F.count(F.lit(1)).over(w_policy_id_by_premium_running).alias("running_count"),
        F.round(F.percent_rank().over(w_adjuster_id_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.sum("claimed_amount").over(w_adjuster_id_by_claim_date_ru).alias("running_claimed_amount"),
        F.dense_rank().over(w_adjuster_id_by_claim_date_ti).alias("claim_date_dense_rank"),
        F.lead("premium").over(w_policy_id_by_claimed_amount).alias("next_premium"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.round(F.col("premium") - F.avg("premium").over(w_claim_date_month), 2).alias("premium_vs_claim_date_month_avg"),
        F.rank().over(w_claim_date_year_by_running_c).alias("running_claimed_amount_rank"),
        F.min("running_count").over(w_claim_date_year_by_running_c_2).alias("min_running_count_so_far"),
        F.count(F.lit(1)).over(w_claim_date_month).alias("rows_in_claim_date_month"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "premium",
        "premium_vs_status_product_avg",
        "running_count",
        "claim_date_percent_rank",
        "running_claimed_amount",
        "claim_date_dense_rank",
        "next_premium",
        "premium_vs_claim_date_month_avg",
        "running_claimed_amount_rank",
        "min_running_count_so_far",
        F.when(F.col("premium") >= 3521.8, "large").when(F.col("premium") >= 1132.1, "medium").otherwise("small").alias("premium_tier"),
    )
    .orderBy(F.col("running_count").desc())
)
