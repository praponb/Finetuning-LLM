from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "product_group").orderBy(F.col("start_date").desc(), F.col("id").desc())

scoped = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("start_date") >= F.lit("2024-08-27").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("HOME", "HEALTH"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
    )
)

latest = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    latest
    .select(
        "*",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown_2"),
    )
)

enriched = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.status_or_unknown",
        "s.status_or_unknown_2",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

line_level = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") >= 0.0), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.status_or_unknown",
        "s.holder_segment",
        "s.holder_name",
        "s.holder_region",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    line_level
    .filter(~F.col("status_or_unknown").isin("LAPSED", "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "product",
        "holder_segment",
        "holder_name",
        "claim_id",
        "claim_reported_at",
        "claim_approved_amount",
    )
    .orderBy(F.col("product").desc(), F.col("id").desc())
)
