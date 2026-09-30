from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_team_by_hired_on = Window.partitionBy("tier", "team").orderBy(F.col("hired_on").desc(), "id")
w_tier_team_by_hired_on_full = Window.partitionBy("tier", "team").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_support_agents
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.when(F.col("team").isin("VIP", "Onboarding"), "group_a").when(F.col("team") == "Billing", "group_b").otherwise("other").alias("team_group"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.rank().over(w_tier_team_by_hired_on).alias("hired_on_rank"),
        F.first("hired_on").over(w_tier_team_by_hired_on_full).alias("first_hired_on"),
        F.round(F.cume_dist().over(w_tier_team_by_hired_on), 3).alias("hired_on_cume_dist"),
        F.nth_value("hired_on", 2).over(w_tier_team_by_hired_on_full).alias("second_hired_on"),
        F.lead("hired_on", 3).over(w_tier_team_by_hired_on).alias("next3_hired_on"),
    )
)

tagged = (
    sequenced
    .select(
        "*",
        (F.col("hired_on_cume_dist") + F.col("hired_on_rank")).alias("hired_on_cume_dist_plus_hired_on_rank"),
        F.length(F.trim(F.col("team_group"))).alias("team_group_len"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

qualified = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left_semi")
)

without_matches = (
    qualified
    .crossJoin(qualified.agg(F.avg("team_group_len").alias("overall_avg_team_group_len")))
    .filter(F.col("team_group_len") > F.col("overall_avg_team_group_len"))
    .drop("overall_avg_team_group_len")
)

annotated = (
    without_matches
    .select(
        "*",
        F.round(F.col("hired_on_cume_dist") * 1.5, 2).alias("hired_on_cume_dist_adj"),
        F.concat_ws(": ", F.col("team"), F.col("team_slug")).alias("team_team_slug_label"),
    )
)

result = (
    annotated
    .filter(F.year(F.col("second_hired_on")) == 2024)
    .select(
        "id",
        "tier",
        "team_group",
        "first_hired_on",
        "hired_on_cume_dist",
        "team_slug",
        "team_team_slug_label",
        F.when(F.col("hired_on_cume_dist_adj") >= 1.0, "HIGH").when(F.col("hired_on_cume_dist_adj") >= 0.6, "MEDIUM").otherwise("LOW").alias("hired_on_cume_dist_adj_tier"),
        F.when(F.col("team").isin("Tech", "Billing"), "group_a").when(F.col("team") == "Onboarding", "group_b").otherwise("other").alias("team_group_2"),
    )
    .orderBy(F.col("first_hired_on").asc_nulls_last(), F.col("team_slug").desc())
)
