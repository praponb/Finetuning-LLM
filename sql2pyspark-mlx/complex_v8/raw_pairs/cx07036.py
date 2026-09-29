from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_teams_hired_on_mont = Window.partitionBy("distinct_teams", F.to_date("hired_on_month")).orderBy("distinct_teams", "hired_on_month", "hired_on_year_ym", "team")
w_distinct_teams_hired_on_mont_2 = Window.partitionBy("distinct_teams", F.to_date("hired_on_month")).orderBy("distinct_teams", "hired_on_month", "hired_on_year_ym", "team").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_support_agents
    .filter(F.col("tier") == "T3")
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.date_trunc("year", F.col("hired_on")).alias("hired_on_year"),
        F.lower(F.trim(F.col("team"))).alias("team_clean"),
    )
)

labeled = (
    filtered
    .withColumn("agent_name_ref", F.concat(F.col("agent_name"), F.lit("#"), F.col("id").cast("string")))
)

summary = (
    labeled
    .groupBy(
        F.date_trunc("month", "hired_on").alias("hired_on_month"),
        F.date_format("hired_on_year", "yyyy-MM").alias("hired_on_year_ym"),
        "team",
    )
    .agg(
        F.sum(F.when(F.col("team").isin("Tech", "Billing", "Onboarding"), 1).otherwise(0)).alias("n_team_in_tech_billing_onboard"),
        F.countDistinct("team").alias("distinct_teams"),
    )
)

windowed = (
    summary
    .withColumn("distinct_teams_percent_rank", F.round(F.percent_rank().over(w_distinct_teams_hired_on_mont), 3))
    .withColumn("second_hired_on_month", F.nth_value("hired_on_month", 2).over(w_distinct_teams_hired_on_mont_2))
)

filtered_2 = (
    windowed
    .crossJoin(windowed.agg(F.avg("n_team_in_tech_billing_onboard").alias("overall_avg_n_team_in_tech_billing_onboard")))
    .filter(F.col("n_team_in_tech_billing_onboard") > F.col("overall_avg_n_team_in_tech_billing_onboard"))
    .drop("overall_avg_n_team_in_tech_billing_onboard")
)

with_activity_2 = (
    filtered_2
    .crossJoin(filtered_2.agg(F.max("distinct_teams").alias("overall_max_distinct_teams")))
    .filter(F.col("distinct_teams") < F.col("overall_max_distinct_teams"))
    .drop("overall_max_distinct_teams")
)

result = (
    with_activity_2
    .filter(F.col("team") == "Billing")
    .select(
        "hired_on_month",
        "hired_on_year_ym",
        "team",
        "n_team_in_tech_billing_onboard",
        "second_hired_on_month",
        F.when(F.col("team").isin("Billing", "Onboarding"), "group_a").when(F.col("team") == "VIP", "group_b").otherwise("other").alias("team_group"),
        F.lower(F.trim(F.col("hired_on_year_ym"))).alias("hired_on_year_ym_clean"),
    )
    .orderBy(F.col("team").asc())
)
