from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_policy_count_ties = Window.orderBy("policy_count")
w_distinct_ids = Window.partitionBy("distinct_ids")
w_all_by_policy_count_full = Window.orderBy("policy_count", "product", "start_date_year", "holder_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_policy_count = Window.orderBy("policy_count", "product", "start_date_year", "holder_id")

cleaned = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-02-14").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

totals = (
    cleaned
    .groupBy("product", F.year("start_date").alias("start_date_year"), "holder_id")
    .agg(F.count("*").alias("cnt"), F.countDistinct("id").alias("distinct_ids"))
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
        F.sum("premium").alias("total_policy_premium"),
    )
)

summarized_rows = (
    totals.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
        "p.total_policy_premium",
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count_3"),
        F.sum("premium").alias("total_policy_premium_2"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count_3"), F.lit(0)).alias("policy_count_3"),
        "p.total_policy_premium_2",
    )
)

filtered = (
    with_counts.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("G%")), "left_semi")
)

metrics = (
    filtered
    .select(
        "product",
        "start_date_year",
        "holder_id",
        "cnt",
        "policy_count",
        "total_policy_premium",
        "policy_count_3",
        F.round(F.cume_dist().over(w_all_by_policy_count_ties), 3).alias("policy_count_cume_dist"),
        F.sum("policy_count_3").over(w_distinct_ids).alias("distinct_ids_sum_policy_count_3"),
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
        F.size(F.collect_set("holder_id").over(w_distinct_ids)).alias("distinct_holder_id_in_distinct_ids"),
        F.nth_value("start_date_year", 2).over(w_all_by_policy_count_full).alias("second_start_date_year"),
        (F.col("total_policy_premium_2") - F.lag("total_policy_premium_2").over(w_all_by_policy_count)).alias("total_policy_premium_2_change"),
    )
)

result = (
    metrics
    .select(
        "product",
        "start_date_year",
        "cnt",
        "rows_in_distinct_ids",
    )
    .unpivot(
        ids=["product", "start_date_year"],
        values=["cnt", "rows_in_distinct_ids"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
