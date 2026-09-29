from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter((~F.col("status").isin("ACTIVE", "CANCELLED")) & (F.col("premium").between(2096.6, 4417.0)))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
    )
)

filtered = (
    scoped.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.year(F.col("p.end_date")) == 2026) & (F.col("p.start_date").between(F.lit("2024-08-27").cast("date"), F.lit("2025-05-02").cast("date"))), "left_anti")
)

enriched = (
    filtered
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "product_len",
        "start_date_year",
        "end_date_label",
        F.round(F.col("product_len") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("product_len_per_premium"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.start_date_year",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    detail_rows
    .groupBy("claim_id")
    .pivot("claim_status", ["PAID", "REJECTED"])
    .agg(F.avg("claim_approved_amount").alias("avg_v"), F.max("claim_approved_amount").alias("max_v"))
    .orderBy("claim_id")
)
