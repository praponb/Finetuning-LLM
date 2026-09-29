from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_hired_on_full = Window.partitionBy("team").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_hired_on = Window.partitionBy("team").orderBy("hired_on", "id")
w_team_by_hired_on_2 = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id")
w_latest = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), F.col("id").desc())

filtered = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 7)
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.substring(F.trim(F.col("agent_name")), 1, 2).alias("agent_name_prefix"),
    )
)

ranked = (
    filtered
    .select(
        "id",
        "team",
        "hired_on",
        F.first("hired_on").over(w_team_by_hired_on_full).alias("first_hired_on"),
        F.lag("hired_on", 2).over(w_team_by_hired_on).alias("prev2_hired_on"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_team_by_hired_on_2)).alias("days_since_prev_hired_on"),
        F.lead("hired_on").over(w_team_by_hired_on).alias("next_hired_on"),
    )
)

qualified = (
    ranked.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.month(F.col("t.closed_at")).isin(1, 3)), "left_anti")
)

deduped = (
    qualified
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

grouped = (
    deduped
    .groupBy(
        "team",
        F.date_format("first_hired_on", "yyyy-MM").alias("first_hired_on_ym"),
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("team").isin("Billing", "VIP"), 1)) / F.count("*"), 1).alias("pct_team_not_in_billing_vip"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.round(F.avg("days_since_prev_hired_on"), 2).alias("avg_days_since_prev_hired_on"),
        F.min("first_hired_on").alias("first_first_hired_on"),
        F.min("days_since_prev_hired_on").alias("min_days_since_prev_hired_on"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "team",
        "first_hired_on_ym",
        "pct_team_not_in_billing_vip",
        "team_list",
        "avg_days_since_prev_hired_on",
        "first_first_hired_on",
        "min_days_since_prev_hired_on",
        "distinct_ids",
        F.quarter(F.col("first_first_hired_on")).alias("first_first_hired_on_quarter"),
        F.when(F.col("team").isin("VIP", "Onboarding"), "group_a").when(F.col("team") == "Billing", "group_b").otherwise("other").alias("team_group"),
    )
    .orderBy(
        F.col("distinct_ids").asc(),
        F.col("avg_days_since_prev_hired_on").asc(),
    )
)
