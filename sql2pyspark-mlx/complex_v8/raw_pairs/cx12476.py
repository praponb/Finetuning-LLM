from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id").orderBy(F.col("policy_end_date").desc(), F.col("id").desc())

valid_claims = (
    df_claims
    .filter((F.col("claim_type").isin("flood", "theft")) & (F.col("approved_amount") > 2272.7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
    )
)

enriched_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("G%")), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "a.adjuster_name",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

filtered = (
    enriched_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()), "left_anti")
    .filter(F.col("adjuster_id").isNotNull())
)

unique_rows = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .rollup("policy_id", "adjuster_id")
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("cnt"))
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
