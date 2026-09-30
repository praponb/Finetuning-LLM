from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count = Window.partitionBy("row_count")
w_distinct_ids_row_count_by_di = Window.partitionBy("distinct_ids", "row_count").orderBy(F.col("distinct_ids").desc(), "claim_date_month", "start_date_ym", "claim_type")
w_row_count_by_row_count_ties = Window.partitionBy("row_count").orderBy("row_count")
w_row_count_by_row_count_full = Window.partitionBy("row_count").orderBy("row_count", "claim_date_month", "start_date_ym", "claim_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") != "EXPIRED"), "left_anti")
)

screened = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id") <= 3) & (F.col("c.claim_date") >= F.lit("2024-08-18").cast("date")), "left_semi")
    .filter(F.col("premium") < 4678.8)
)

detail_rows = (
    screened.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") >= F.lit("2024-07-22").cast("date")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

totals = (
    detail_rows
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        "claim_type",
    )
    .agg(
        F.round(F.sum("claim_claimed_amount"), 2).alias("total_claim_claimed_amount"),
        F.count("*").alias("row_count"),
        F.sum("claim_claimed_amount").alias("total_claim_claimed_amount_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

ranked = (
    totals
    .select(
        "claim_date_month",
        "start_date_ym",
        "claim_type",
        "total_claim_claimed_amount",
        "row_count",
        F.round(F.col("row_count") / F.nullif(F.sum("row_count").over(w_row_count), F.lit(0)), 4).alias("row_count_share_of_row_count"),
        F.round(F.col("total_claim_claimed_amount") / F.nullif(F.sum("total_claim_claimed_amount").over(w_row_count), F.lit(0)), 4).alias("total_claim_claimed_amount_share_of_row_count"),
        F.dense_rank().over(w_distinct_ids_row_count_by_di).alias("distinct_ids_dense_rank"),
        F.round(F.percent_rank().over(w_row_count_by_row_count_ties), 3).alias("row_count_percent_rank"),
        F.first("distinct_ids").over(w_row_count_by_row_count_full).alias("first_distinct_ids"),
    )
)

result = (
    ranked
    .select(
        "claim_date_month",
        "claim_type",
        "total_claim_claimed_amount",
        "row_count",
        "row_count_share_of_row_count",
        "total_claim_claimed_amount_share_of_row_count",
        "distinct_ids_dense_rank",
        "row_count_percent_rank",
        F.date_trunc("quarter", F.col("claim_date_month")).alias("claim_date_month_quarter"),
        F.when(F.col("claim_type").isin("theft", "fire"), "group_a").when(F.col("claim_type") == "delay", "group_b").otherwise("other").alias("claim_type_group"),
    )
    .orderBy(F.col("total_claim_claimed_amount").desc())
)
