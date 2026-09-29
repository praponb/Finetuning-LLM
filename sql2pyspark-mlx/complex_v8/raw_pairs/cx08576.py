from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_start_date_plus_3m = Window.partitionBy("status").orderBy(F.col("start_date_plus_3m").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_start_date_plus = Window.partitionBy("holder_id").orderBy(F.col("start_date_plus_3m").desc(), "id", "claim_id")
w_holder_id_by_start_date_plus_2 = Window.partitionBy("holder_id").orderBy(F.col("start_date_plus_3m").desc())
w_status_by_start_date_plus_3m_2 = Window.partitionBy("status").orderBy(F.col("start_date_plus_3m").desc(), "id", "claim_id")

prepared = (
    df_policies
    .filter((F.col("product").isNotNull()) & (F.col("status").isin("LAPSED", "CANCELLED", "EXPIRED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.when(F.col("product").isin("TRAVEL", "HEALTH"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
        F.add_months(F.col("start_date"), 3).alias("start_date_plus_3m"),
    )
)

line_level_claims = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") == "collision"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.product_group",
        "s.start_date_plus_3m",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_date",
        "c.claim_type",
    )
)

metrics = (
    line_level_claims
    .select(
        "*",
        F.nth_value("claim_date", 2).over(w_status_by_start_date_plus_3m).alias("second_claim_date"),
        F.lag("start_date_plus_3m").over(w_holder_id_by_start_date_plus).alias("prev_start_date_plus_3m"),
        F.round(F.percent_rank().over(w_holder_id_by_start_date_plus_2), 3).alias("start_date_plus_3m_percent_rank"),
        F.lag("claim_date", 2).over(w_status_by_start_date_plus_3m_2).alias("prev2_claim_date"),
        F.round(F.cume_dist().over(w_holder_id_by_start_date_plus), 3).alias("start_date_plus_3m_cume_dist"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count(F.when(F.col("status") == "LAPSED", 1)).alias("policy_status_count"),
        F.count("*").alias("policy_count_2"),
    )
)

profiled = (
    metrics.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

per_group = (
    profiled
    .groupBy("claim_type", "product_group", "claim_id")
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    per_group
    .select(
        "claim_type",
        "product_group",
        "claim_id",
        "status_list",
        "n_rows",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("product_group")), " ", "_")).alias("product_group_slug"),
    )
)
