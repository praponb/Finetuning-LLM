from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_policy_end_date_month_p = Window.partitionBy(F.date_trunc("month", "last_policy_end_date"), "policy_count_2").orderBy(F.col("end_date_quarter").desc(), "id").rowsBetween(-2, Window.currentRow)
w_status = Window.partitionBy("status")
w_last_policy_end_date_month_p_2 = Window.partitionBy(F.date_trunc("month", "last_policy_end_date"), "policy_count_2").orderBy(F.col("end_date_quarter").desc())
w_last_policy_end_date_month_p_3 = Window.partitionBy(F.date_trunc("month", "last_policy_end_date"), "policy_count_2").orderBy(F.col("end_date_quarter").desc(), "id")
w_last_policy_end_date_month_p_4 = Window.partitionBy(F.date_trunc("month", "last_policy_end_date"), "policy_count_2").orderBy(F.col("end_date_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_policies
    .filter(F.col("product").isin("HEALTH", "LIFE", "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.max("premium").alias("max_policy_premium"),
    )
)

profiled = (
    prepared.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.max_policy_premium",
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("start_date").alias("last_policy_start_date"),
        F.count("*").alias("policy_count_2"),
        F.max("end_date").alias("last_policy_end_date"),
    )
)

with_counts = (
    profiled.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.last_policy_start_date",
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
        "p.last_policy_end_date",
    )
)

windowed = (
    with_counts
    .select(
        "*",
        F.round(F.avg("premium_adj").over(w_last_policy_end_date_month_p), 2).alias("premium_adj_avg_3"),
        F.size(F.collect_set("holder_id").over(w_status)).alias("distinct_holder_id_in_status"),
        F.round(F.percent_rank().over(w_last_policy_end_date_month_p_2), 3).alias("end_date_quarter_percent_rank"),
        F.round(F.col("premium_adj") / F.nullif(F.sum("premium_adj").over(w_status), F.lit(0)), 4).alias("premium_adj_share_of_status"),
        F.lead("policy_count", 3, 0).over(w_last_policy_end_date_month_p_3).alias("next3_policy_count"),
        F.first("start_date").over(w_last_policy_end_date_month_p_4).alias("first_start_date"),
    )
)

result = (
    windowed
    .groupingSets(
        [["policy_count", "status", "product"], ["policy_count"]],
        "policy_count", "status", "product",
    )
    .agg(
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("n_status_expired"),
        F.round(100 * F.count(F.when(F.col("status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_status_is_not_null"),
    )
    .orderBy(
        F.col("policy_count").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
