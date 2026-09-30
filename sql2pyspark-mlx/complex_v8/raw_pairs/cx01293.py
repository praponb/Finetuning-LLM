from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("segment", "region").orderBy("risk_score", "id")
w_policy_id = Window.partitionBy("policy_id")

scoped = (
    df_policyholders
    .filter(F.col("risk_score") <= 0.6)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.substring(F.trim(F.col("segment")), 1, 3).alias("segment_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
        F.date_format(F.col("joined_on"), "yyyy-MM-dd").alias("joined_on_label"),
    )
)

deduped = (
    scoped
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_children_policys = (
    deduped.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_name",
        "s.region",
        "s.risk_score",
        "s.segment_prefix",
        "s.days_since_joined_on",
        "s.joined_on_label",
        F.col("p.id").alias("policy_id"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

labeled = (
    with_children_policys
    .select(
        "*",
        F.concat(F.col("policy_status"), F.lit("#"), F.col("days_since_joined_on").cast("string")).alias("policy_status_ref"),
        F.date_format(F.col("policy_end_date"), "yyyy-MM").alias("policy_end_date_label"),
    )
)

metrics = (
    labeled
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on_label",
        "policy_id",
        "policy_status",
        "policy_start_date",
        "policy_end_date_label",
        F.round(F.col("days_since_joined_on") - F.avg("days_since_joined_on").over(w_policy_id), 2).alias("days_since_joined_on_vs_policy_id_avg"),
        F.round(F.avg("risk_score").over(w_policy_id), 2).alias("policy_id_avg_risk_score"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on_label",
        "policy_id",
        "policy_status",
        "policy_start_date",
        "policy_id_avg_risk_score",
        "rows_in_policy_id",
    )
)
