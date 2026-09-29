from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status", "holder_id").orderBy(F.col("policy_count_2").desc(), F.col("id").desc())
w_all_by_first_policy_end_date = Window.orderBy(F.col("first_policy_end_date").desc(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_status_policy_count_2_by_sta = Window.partitionBy("status", "policy_count_2").orderBy("start_date", "id")

scoped_policys = (
    df_policies
    .filter((F.col("holder_id") <= 11) & (F.col("start_date") < F.lit("2025-03-23").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("end_date").alias("first_policy_end_date"),
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

profiled = (
    scoped_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.first_policy_end_date",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

unique_rows = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    unique_rows
    .select(
        "*",
        (F.col("policy_count_2") - F.lag("policy_count_2").over(w_all_by_first_policy_end_date)).alias("policy_count_2_change"),
        F.round(F.col("policy_count_2") - F.avg("policy_count_2").over(w_holder_id), 2).alias("policy_count_2_vs_holder_id_avg"),
        F.lead("end_date", 2).over(w_status_policy_count_2_by_sta).alias("next2_end_date"),
    )
)

detail_rows = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.policy_count",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

enriched = (
    detail_rows
    .filter(F.col("claim_approved_amount") <= 14668.0)
    .select(
        "*",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.upper(F.trim(F.col("claim_status"))).alias("claim_status_clean"),
    )
)

exploded_detail = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.policy_count",
        "s.claim_id",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_type",
        "c.claim_date",
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "policy_count",
        "claim_id",
        "claim_type",
        "claim_date",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
    .orderBy(F.col("policy_count").desc())
)
