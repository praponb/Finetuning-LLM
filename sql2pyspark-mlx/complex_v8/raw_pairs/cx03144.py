from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy(F.col("start_date").desc(), F.col("id").desc())

cleaned_policys = (
    df_policies
    .filter(F.col("product") != "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
    )
)

latest = (
    cleaned_policys
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined = (
    latest.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.region") != "West"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "p.holder_name",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

labeled = (
    joined
    .filter(F.col("product") == "AUTO")
    .select(
        "*",
        F.coalesce(F.col("holder_risk_score"), F.lit(0.0)).alias("holder_risk_score_filled"),
    )
)

with_parent = (
    labeled.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") == "VIP"), "inner")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.holder_name").alias("holder_holder_name"),
        F.col("p.region").alias("holder_region"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

summarized_rows = (
    with_parent.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region_2"),
        F.col("p.holder_name").alias("holder_holder_name_2"),
    )
)

result = (
    combined_details
    .filter(F.col("holder_region_2") == "Central")
    .select(
        "id",
        "holder_id",
        "end_date",
        "holder_name",
        "holder_risk_score",
        "holder_risk_score_filled",
        "holder_joined_on",
        "holder_region",
        "claim_count",
        "claim_count_2",
        "holder_segment",
        "holder_region_2",
        "holder_holder_name_2",
    )
    .orderBy("holder_segment")
)
