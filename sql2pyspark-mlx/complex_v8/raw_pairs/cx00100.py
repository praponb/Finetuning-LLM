from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_team").orderBy("approved_amount_adj", "id")
w_latest = Window.partitionBy("status", "claim_type_group").orderBy(F.col("approved_amount").desc(), F.col("id").desc())

recent = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-04-17").cast("date")) & (F.col("claimed_amount") >= 17585.2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.when(F.col("claim_type").isin("flood", "theft"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
        F.round(F.col("claimed_amount") * 1.07, 2).alias("claimed_amount_adj"),
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
    )
)

combined_details_adjuster = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

latest = (
    combined_details_adjuster
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_2 = (
    latest
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_2
    .rollup("adjuster_team", "claim_type", "adjuster_id")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
    )
    .select(
        F.coalesce(F.col("adjuster_team"), F.lit("ALL")).alias("adjuster_team"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "adjuster_id",
        "adjuster_name_list",
    )
)
