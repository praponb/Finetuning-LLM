from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("claimed_amount") < 43500.4) & (F.to_date(F.col("reported_at")) < F.lit("2025-03-08").cast("date")))
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
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

stats = (
    filtered_claims
    .filter(F.col("claim_date_month_start") >= F.lit("2024-11-01").cast("date"))
    .groupBy(F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"))
    .agg(F.array_sort(F.collect_set("status")).alias("status_list"))
)

per_label = stats.select("claim_date_ym", F.explode("status_list").alias("status_list_item"))

result = (
    per_label
    .select(
        "claim_date_ym",
        "status_list_item",
        F.concat_ws(": ", F.col("claim_date_ym"), F.col("status_list_item")).alias("claim_date_ym_status_list_item_label"),
    )
    .orderBy(
        F.col("status_list_item").desc(),
        F.col("claim_date_ym_status_list_item_label").asc_nulls_last(),
        "claim_date_ym",
    )
    .limit(20)
)
