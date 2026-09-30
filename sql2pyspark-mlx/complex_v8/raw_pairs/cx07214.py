from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id", "claim_status").orderBy("claim_approved_amount", "id", "claim_id")

cleaned = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-12-05").cast("date")) | (F.col("start_date") < F.lit("2025-03-21").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
    )
)

unique_rows = (
    detail_rows
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .groupingSets(
        [["claim_status", "claim_id"], ["claim_status"], ["claim_id"], []],
        "claim_status", "claim_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("cnt"),
        F.min("premium").alias("min_premium"),
        F.percentile_approx("claim_approved_amount", 0.5).alias("median_claim_approved_amount"),
    )
)
