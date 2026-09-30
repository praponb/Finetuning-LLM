from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("claim_date").desc(), F.col("id").desc())
w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_policy_id_claim_type_clean_l = Window.partitionBy("policy_id", "claim_type_clean_len").orderBy(F.col("reported_at").desc(), "id")

cleaned_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 5313.8) & (F.col("claim_date").between(F.lit("2024-02-17").cast("date"), F.lit("2025-05-30").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

labeled = (
    cleaned_claims
    .select(
        "*",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
        F.length(F.trim(F.col("claim_type_clean"))).alias("claim_type_clean_len"),
    )
)

latest_per_key = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.max("claimed_amount").over(w_claim_date_month).alias("claim_date_month_max_claimed_amount"),
        F.count(F.lit(1)).over(w_claim_date_month).alias("rows_in_claim_date_month"),
        F.ntile(3).over(w_policy_id_claim_type_clean_l).alias("reported_at_tercile"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_month), 2).alias("claimed_amount_vs_claim_date_month_avg"),
    )
)

rollup_base = (
    metrics
    .groupBy("claim_type", "claim_date_dayofweek", "policy_id")
    .agg(
        F.sum("claim_date_month_max_claimed_amount").alias("total_claim_date_month_max_claimed_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(F.sum("claim_date_month_max_claimed_amount") / F.nullif(F.sum("reported_at_tercile"), F.lit(0)), 3).alias("claim_date_month_max_claimed_amount_to_reported_"),
        F.round(F.avg("rows_in_claim_date_month"), 2).alias("avg_rows_in_claim_date_month"),
    )
)

result = (
    rollup_base
    .groupBy("policy_id")
    .pivot("claim_type", ["theft", "fire"])
    .agg(F.max("claim_date_dayofweek").alias("max_v"), F.count("claim_date_dayofweek").alias("n"))
    .orderBy("policy_id")
)
