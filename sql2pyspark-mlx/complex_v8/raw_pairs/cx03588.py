from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_by_hired_on = Window.partitionBy("tier").orderBy("hired_on", "id")
w_tier_by_hired_on_ties = Window.partitionBy("tier").orderBy("hired_on")
w_tier_by_hired_on_running = Window.partitionBy("tier").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tier_by_hired_on_running_2 = Window.partitionBy("tier").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_hired_on_year = Window.partitionBy("last_known_hired_on_year")
w_seq_no_last_known_hired_on_y = Window.partitionBy("seq_no", "last_known_hired_on_year")
w_tier_hired_on_month_by_hired = Window.partitionBy("tier", F.date_trunc("month", "hired_on")).orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_hired_on_year_by = Window.partitionBy("last_known_hired_on_year").orderBy(F.col("hired_on_percent_rank_adj").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_support_agents
    .filter((~F.col("tier").isin("T1", "T3")) & (F.col("hired_on") >= F.lit("2024-05-02").cast("date")))
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.row_number().over(w_tier_by_hired_on).alias("seq_no"),
        F.round(F.cume_dist().over(w_tier_by_hired_on_ties), 3).alias("hired_on_cume_dist"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_tier_by_hired_on)).alias("days_since_prev_hired_on"),
        F.round(F.percent_rank().over(w_tier_by_hired_on), 3).alias("hired_on_percent_rank"),
        F.lead("hired_on").over(w_tier_by_hired_on).alias("next_hired_on"),
        F.first("hired_on", ignorenulls=True).over(w_tier_by_hired_on_running).alias("first_known_hired_on"),
        F.last("hired_on", ignorenulls=True).over(w_tier_by_hired_on_running_2).alias("last_known_hired_on"),
    )
)

enriched = (
    with_trends
    .select(
        "*",
        F.year(F.col("last_known_hired_on")).alias("last_known_hired_on_year"),
        F.date_format(F.col("hired_on"), "yyyy-MM").alias("hired_on_label"),
        F.round(F.col("hired_on_percent_rank") * 1.5, 2).alias("hired_on_percent_rank_adj"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("days_since_prev_hired_on") - F.avg("days_since_prev_hired_on").over(w_last_known_hired_on_year), 2).alias("days_since_prev_hired_on_vs_last_known_hired_on_"),
        F.round(F.col("days_since_prev_hired_on") / F.nullif(F.sum("days_since_prev_hired_on").over(w_seq_no_last_known_hired_on_y), F.lit(0)), 4).alias("days_since_prev_hired_on_share_of_seq_no_last_kn"),
        F.round(F.col("last_known_hired_on_year") - F.avg("last_known_hired_on_year").over(w_last_known_hired_on_year), 2).alias("last_known_hired_on_year_vs_last_known_hired_on_"),
        F.last("days_since_prev_hired_on", ignorenulls=True).over(w_tier_hired_on_month_by_hired).alias("last_known_days_since_prev_hired_on"),
        F.max("seq_no").over(w_seq_no_last_known_hired_on_y).alias("seq_no_last_known_hired_on_year_max_seq_no"),
        F.nth_value("hired_on_cume_dist", 2).over(w_last_known_hired_on_year_by).alias("second_hired_on_cume_dist"),
    )
)

result = (
    ranked
    .filter(F.col("days_since_prev_hired_on") <= 160)
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        "hired_on_cume_dist",
        "days_since_prev_hired_on",
        "first_known_hired_on",
        "last_known_hired_on",
        "last_known_hired_on_year",
        "hired_on_label",
        "hired_on_percent_rank_adj",
        "days_since_prev_hired_on_share_of_seq_no_last_kn",
        "last_known_hired_on_year_vs_last_known_hired_on_",
        "last_known_days_since_prev_hired_on",
        "seq_no_last_known_hired_on_year_max_seq_no",
        "second_hired_on_cume_dist",
        F.abs(F.col("seq_no") - 5).alias("seq_no_distance"),
        F.concat(F.col("agent_name"), F.lit("#"), F.col("last_known_hired_on_year").cast("string")).alias("agent_name_ref"),
    )
    .orderBy("hired_on", F.col("agent_name").asc())
)
