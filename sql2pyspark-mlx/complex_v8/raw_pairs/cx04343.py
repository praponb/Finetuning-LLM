from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_group_by_hired_on = Window.partitionBy("team_group").orderBy("hired_on", "id")
w_team_group_tier_by_hired_on = Window.partitionBy("team_group", "tier").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_group_tier_by_hired_on_2 = Window.partitionBy("team_group", "tier").orderBy("hired_on")
w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_group_tier_by_hired_on_3 = Window.partitionBy("team_group", "tier").orderBy("hired_on", "id")
w_team_group_tier_by_hired_on_4 = Window.partitionBy("team_group", "tier").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc(), "id")
w_team_group_by_hired_on_full = Window.partitionBy("team_group").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_hired_on_cume_dist_t = Window.partitionBy("team").orderBy(F.col("hired_on_cume_dist").desc())
w_team_by_seq_no = Window.partitionBy("team").orderBy(F.col("seq_no").desc(), "id")
w_team_by_hired_on_cume_dist_m = Window.partitionBy("team").orderBy(F.col("hired_on_cume_dist").desc(), "id").rowsBetween(-6, Window.currentRow)
w_prev_hired_on_month = Window.partitionBy(F.date_trunc("month", "prev_hired_on"))
w_team_by_hired_on_cume_dist = Window.partitionBy("team").orderBy(F.col("hired_on_cume_dist").desc(), "id")

recent_agents = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%wo%")) & (F.col("team") == "Tech"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.date_format(F.col("hired_on"), "yyyy-MM-dd").alias("hired_on_label"),
        F.when(F.col("team").isin("Billing", "Tech"), "group_a").when(F.col("team") == "VIP", "group_b").otherwise("other").alias("team_group"),
    )
)

metrics = (
    recent_agents
    .select(
        "*",
        F.round(F.cume_dist().over(w_team_group_by_hired_on), 3).alias("hired_on_cume_dist"),
        F.nth_value("hired_on", 2).over(w_team_group_tier_by_hired_on).alias("second_hired_on"),
        F.dense_rank().over(w_team_group_tier_by_hired_on_2).alias("hired_on_dense_rank"),
        F.last("hired_on").over(w_all_by_hired_on_full).alias("last_hired_on"),
        F.lead("hired_on", 3).over(w_team_group_by_hired_on).alias("next3_hired_on"),
        F.row_number().over(w_team_group_tier_by_hired_on_3).alias("seq_no"),
        F.ntile(10).over(w_team_group_tier_by_hired_on_3).alias("hired_on_decile"),
        F.last("hired_on", ignorenulls=True).over(w_team_group_tier_by_hired_on_4).alias("last_known_hired_on"),
        F.lag("hired_on").over(w_team_group_by_hired_on).alias("prev_hired_on"),
        F.round(F.cume_dist().over(w_team_group_tier_by_hired_on_2), 3).alias("hired_on_cume_dist_2"),
        F.round(F.cume_dist().over(w_all_by_hired_on), 3).alias("hired_on_cume_dist_3"),
        F.nth_value("hired_on", 2).over(w_team_group_by_hired_on_full).alias("second_hired_on_2"),
        F.lag("hired_on").over(w_all_by_hired_on).alias("prev_hired_on_2"),
        F.ntile(10).over(w_all_by_hired_on).alias("hired_on_decile_2"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w_team_by_hired_on_cume_dist_t).alias("hired_on_cume_dist_dense_rank"),
        F.lead("second_hired_on").over(w_team_by_seq_no).alias("next_second_hired_on"),
        F.sum("hired_on_decile").over(w_team_by_hired_on_cume_dist_m).alias("hired_on_decile_sum_7"),
        F.round(F.col("hired_on_cume_dist_2") - F.avg("hired_on_cume_dist_2").over(w_prev_hired_on_month), 2).alias("hired_on_cume_dist_2_vs_prev_hired_on_month_avg"),
        F.sum("hired_on_cume_dist_2").over(w_prev_hired_on_month).alias("prev_hired_on_month_sum_hired_on_cume_dist_2"),
        F.lag("last_hired_on", 3).over(w_team_by_seq_no).alias("prev3_last_hired_on"),
        F.lag("hired_on", 2).over(w_team_by_hired_on_cume_dist).alias("prev2_hired_on"),
    )
)

result = (
    ranked
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        "hired_on_label",
        "team_group",
        "hired_on_cume_dist",
        "second_hired_on",
        "hired_on_dense_rank",
        "last_hired_on",
        "next3_hired_on",
        "seq_no",
        "hired_on_decile",
        "last_known_hired_on",
        "prev_hired_on",
        "hired_on_cume_dist_2",
        "hired_on_cume_dist_3",
        "prev_hired_on_2",
        "hired_on_decile_2",
        "hired_on_cume_dist_dense_rank",
        "next_second_hired_on",
        "hired_on_decile_sum_7",
        "hired_on_cume_dist_2_vs_prev_hired_on_month_avg",
        "prev_hired_on_month_sum_hired_on_cume_dist_2",
        "prev3_last_hired_on",
        F.concat_ws(" / ", F.col("agent_name"), F.col("hired_on_label")).alias("agent_name_hired_on_label_label"),
    )
    .orderBy(
        F.col("last_known_hired_on").desc(),
        F.col("agent_name_hired_on_label_label").asc(),
        "id",
    )
    .limit(10)
)
