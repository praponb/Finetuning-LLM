from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_latest = Window.partitionBy("status", "holder_region").orderBy("start_date_month", "id")

base_policys = (
    df_policies
    .filter(F.col("premium") > 2111.3)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.month(F.col("start_date")).alias("start_date_month"),
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
    )
)

windowed = (
    base_policys
    .select(
        "*",
        F.round(F.col("start_date_month") / F.nullif(F.sum("start_date_month").over(w_product), F.lit(0)), 4).alias("start_date_month_share_of_product"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
    )
)

enriched = (
    windowed.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") == "SME"), "inner")
    .filter(F.col("p.region") == "North")
    .select(
        "s.*",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
    )
)

profiled = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        "c.last_claim_reported_at",
        "c.avg_claim_claimed_amount",
    )
)

holder_id_stats = (
    profiled
    .groupBy("holder_id")
    .agg(F.max("start_date_month").alias("max_start_date_month"))
)

vs_group = (
    profiled.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.start_date_month") == F.col("g.max_start_date_month"))
    .select(
        "s.*",
        "g.max_start_date_month",
        F.round(F.col("s.start_date_month") - F.col("g.max_start_date_month"), 2).alias("start_date_month_minus_max"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("start_date").alias("last_policy_start_date"))
)

with_stats = (
    vs_group.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.last_policy_start_date")
)

unique_rows = (
    with_stats
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupingSets(
        [["start_date_month", "status"], ["start_date_month"], ["status"]],
        "start_date_month", "status",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("holder_segment").alias("distinct_holder_segments"),
        F.sum(F.when(F.upper(F.trim(F.col("holder_name"))).like("K%"), F.col("start_date_month"))).alias("start_date_month_upper_trim_holder_name_like"),
    )
    .select(
        "start_date_month",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "n_rows",
        "distinct_holder_segments",
        "start_date_month_upper_trim_holder_name_like",
    )
    .orderBy(
        F.col("start_date_month").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
