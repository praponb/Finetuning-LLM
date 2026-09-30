from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_first_claim_report = Window.partitionBy("status").orderBy(F.col("first_claim_reported_at").desc_nulls_first(), "id")
w_status_by_end_date = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")
w_latest = Window.partitionBy("product", "status").orderBy(F.col("first_claim_reported_at_dense_rank").desc(), F.col("id").desc())
w_latest_2 = Window.partitionBy("holder_id").orderBy(F.col("avg_policy_premium").desc(), F.col("id").desc())

valid_policys = (
    df_policies
    .filter(F.col("status") == "EXPIRED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

with_stats = (
    valid_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

labeled = (
    with_stats
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "start_date_label",
        "first_claim_reported_at",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

sequenced = (
    labeled
    .select(
        "*",
        F.dense_rank().over(w_status_by_first_claim_report).alias("first_claim_reported_at_dense_rank"),
        F.row_number().over(w_status_by_end_date).alias("rn"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.count("*").alias("policy_count"),
    )
)

with_stats_2 = (
    sequenced.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.avg_policy_premium",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

latest_per_key = (
    with_stats_2
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    latest_per_key
    .withColumn("rn_2", F.row_number().over(w_latest_2))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

result = (
    deduped
    .select(
        "id",
        F.col("policy_count").alias("policy_count_v"),
        F.col("first_claim_reported_at_dense_rank").alias("first_claim_reported_at_dense_rank_v"),
        F.col("rn").alias("rn_v"),
    )
    .unpivot(
        ids=["id"],
        values=["policy_count_v", "first_claim_reported_at_dense_rank_v", "rn_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
