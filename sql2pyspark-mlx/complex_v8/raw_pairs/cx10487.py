from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_year_by_max_appr = Window.partitionBy("reported_at_year").orderBy("max_approved_amount", "reported_at_year", "claim_type")
w_reported_at_year_by_max_appr_2 = Window.partitionBy("reported_at_year").orderBy("max_approved_amount", "reported_at_year", "claim_type").rowsBetween(-6, -1)
w_reported_at_year_by_max_appr_3 = Window.partitionBy("reported_at_year").orderBy("max_approved_amount")
w_seq_no_reported_at_year_by_m = Window.partitionBy("seq_no", "reported_at_year").orderBy(F.col("max_approved_amount_percent_rank").desc())
w_seq_no_reported_at_year_by_m_2 = Window.partitionBy("seq_no", "reported_at_year").orderBy(F.col("max_approved_amount_percent_rank").desc(), "reported_at_year", "claim_type").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_max_approved_a = Window.partitionBy("claim_type").orderBy(F.col("max_approved_amount_quartile").desc(), "reported_at_year", "claim_type").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_claims = (
    df_claims
    .filter((F.col("policy_id") >= 3) & (F.col("claimed_amount").between(3211.4, 46461.4)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

grouped = (
    filtered_claims
    .filter(F.col("claim_type").isin("theft", "collision", "delay"))
    .groupBy(F.year("reported_at").alias("reported_at_year"), "claim_type")
    .agg(
        F.max("claim_date_month").alias("last_claim_date_month"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.sum("claim_type_len").alias("total_claim_type_len"),
        F.max("approved_amount").alias("max_approved_amount"),
    )
)

sequenced = (
    grouped
    .select(
        "reported_at_year",
        "claim_type",
        "last_claim_date_month",
        "claim_type_list",
        "total_claim_type_len",
        "max_approved_amount",
        F.ntile(4).over(w_reported_at_year_by_max_appr).alias("max_approved_amount_quartile"),
        F.row_number().over(w_reported_at_year_by_max_appr).alias("seq_no"),
        F.max("max_approved_amount").over(w_reported_at_year_by_max_appr_2).alias("max_approved_amount_max_6_prior"),
        F.lag("reported_at_year").over(w_reported_at_year_by_max_appr).alias("prev_reported_at_year"),
        F.round(F.percent_rank().over(w_reported_at_year_by_max_appr_3), 3).alias("max_approved_amount_percent_rank"),
    )
)

without_matches = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("max_approved_amount_quartile").alias("overall_avg_max_approved_amount_quartile")))
    .filter(F.col("max_approved_amount_quartile") > F.col("overall_avg_max_approved_amount_quartile"))
    .drop("overall_avg_max_approved_amount_quartile")
)

windowed = (
    without_matches
    .select(
        "reported_at_year",
        "claim_type",
        "last_claim_date_month",
        "claim_type_list",
        "max_approved_amount",
        "max_approved_amount_quartile",
        "max_approved_amount_max_6_prior",
        "max_approved_amount_percent_rank",
        F.round(F.cume_dist().over(w_seq_no_reported_at_year_by_m), 3).alias("max_approved_amount_percent_rank_cume_dist"),
        F.max("max_approved_amount_quartile").over(w_seq_no_reported_at_year_by_m_2).alias("max_max_approved_amount_quartile_so_far"),
        F.max("prev_reported_at_year").over(w_claim_type_by_max_approved_a).alias("max_prev_reported_at_year_so_far"),
    )
)

screened = (
    windowed
    .crossJoin(windowed.agg(F.min("max_max_approved_amount_quartile_so_far").alias("overall_min_max_max_approved_amount_quartile_so_")))
    .filter(F.col("max_max_approved_amount_quartile_so_far") > F.col("overall_min_max_max_approved_amount_quartile_so_"))
    .drop("overall_min_max_max_approved_amount_quartile_so_")
)

result = (
    screened
    .select(
        "reported_at_year",
        "claim_type",
        "last_claim_date_month",
        "claim_type_list",
        "max_approved_amount_max_6_prior",
        "max_max_approved_amount_quartile_so_far",
        "max_prev_reported_at_year_so_far",
    )
)
