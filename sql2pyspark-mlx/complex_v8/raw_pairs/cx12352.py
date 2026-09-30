from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("status") != "LAPSED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.abs(F.col("premium") - 2824.5).alias("premium_distance"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.concat_ws(" / ", F.col("product"), F.col("status")).alias("product_status_label"),
    )
)

filtered = (
    recent_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") != "delay") & (F.col("c.status") == "REJECTED"), "left_semi")
    .filter(F.col("premium").between(2033.8, 4417.0))
)

joined_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.premium_distance",
        "s.product_status_label",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.region").alias("holder_region"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

with_counts = (
    joined_holder.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

tagged = (
    with_counts
    .select(
        "*",
        F.datediff(F.col("end_date"), F.col("holder_joined_on")).alias("days_holder_joined_on_to_end_date"),
        F.concat_ws(": ", F.col("product_status_label"), F.col("holder_region")).alias("product_status_label_holder_region_label"),
    )
)

result = (
    tagged
    .select("id", "premium_distance", "premium")
    .unpivot(
        ids=["id"],
        values=["premium_distance", "premium"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
