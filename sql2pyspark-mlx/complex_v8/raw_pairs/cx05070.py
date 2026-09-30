from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_type", "claim_status").orderBy(F.col("start_date").desc(), F.col("id").desc(), F.col("claim_id").desc())
w_latest = Window.partitionBy("product", "holder_id").orderBy(F.col("product_len").desc(), F.col("id").desc(), F.col("claim_id").desc())

valid_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

with_children_claims = (
    valid_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") != "theft"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
    )
)

deduped = (
    with_children_claims
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    deduped
    .select(
        "id",
        "holder_id",
        "product",
        "claim_id",
        "claim_date",
        "claim_status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
)

annotated = (
    scored
    .select(
        "id",
        "holder_id",
        "product",
        "claim_id",
        "claim_date",
        "claim_status",
        "product_clean",
        "product_len",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
        F.length(F.trim(F.col("claim_status"))).alias("claim_status_len"),
    )
)

with_parent_claim = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.status") == "APPROVED"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.claim_id",
        "s.claim_date",
        "s.claim_status",
        "s.product_clean",
        "s.product_len",
        "s.days_since_claim_date",
        "s.claim_status_len",
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
    )
)

latest_per_key = (
    with_parent_claim
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "holder_id",
        "product",
        "claim_id",
        "claim_date",
        "claim_status",
        "product_len",
        "days_since_claim_date",
        "claim_status_len",
        "claim_type",
        F.add_months(F.col("claim_date"), 6).alias("claim_date_plus_6m"),
        F.concat_ws("-", F.col("product"), F.col("product_clean")).alias("product_product_clean_label"),
    )
    .orderBy(F.col("claim_status").desc(), F.col("id").desc())
)
