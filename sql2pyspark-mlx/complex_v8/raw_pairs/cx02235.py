from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id", "status").orderBy(F.col("claim_date_quarter").desc(), F.col("id").desc())

prepared_claims = (
    df_claims
    .filter((F.col("claim_type") == "delay") & (F.col("policy_id") >= 6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.add_months(F.col("claim_date"), 1).alias("claim_date_plus_1m"),
    )
)

latest = (
    prepared_claims
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .filter(F.col("claim_date_plus_1m") >= F.lit("2024-05-01").cast("date"))
    .select(
        "*",
        F.coalesce(F.col("claim_date_quarter"), F.lit(0)).alias("claim_date_quarter_filled"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.when(F.col("status").isin("REJECTED", "IN_REVIEW"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
)

result = (
    enriched
    .select(
        "id",
        F.col("claim_date_quarter").alias("m_claim_date_quarter"),
        F.col("claim_date_quarter_filled").alias("claim_date_quarter_filled_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_claim_date_quarter", "claim_date_quarter_filled_v"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
