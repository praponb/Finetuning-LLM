from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_year_tier_by_hired = Window.partitionBy(F.year("hired_on"), "tier").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_hired_on_running = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_year_tier_by_hired_2 = Window.partitionBy(F.year("hired_on"), "tier").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tier_by_hired_on_full = Window.partitionBy("tier").orderBy(F.col("hired_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_by_hired_on_full = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_hired_on_month_by_hired_on = Window.partitionBy(F.date_trunc("month", "hired_on")).orderBy("hired_on", "id")
w_tier_by_hired_on = Window.partitionBy("tier").orderBy(F.col("hired_on").desc_nulls_first(), "id")
w_hired_on_year_tier_by_hired_3 = Window.partitionBy(F.year("hired_on"), "tier").orderBy(F.col("hired_on").desc(), "id")
w_hired_on_month_by_hired_on_t = Window.partitionBy(F.date_trunc("month", "hired_on")).orderBy("hired_on")

prepared = (
    df_support_agents
    .filter(F.lower(F.col("agent_name")).like("%h %"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.lower(F.regexp_replace(F.trim(F.col("tier")), " ", "_")).alias("tier_slug"),
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
    )
)

ranked = (
    prepared
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        "tier_slug",
        "team_ref",
        F.nth_value("hired_on", 2).over(w_hired_on_year_tier_by_hired).alias("second_hired_on"),
        F.last("hired_on", ignorenulls=True).over(w_team_by_hired_on_running).alias("last_known_hired_on"),
        F.last("hired_on", ignorenulls=True).over(w_hired_on_year_tier_by_hired_2).alias("last_known_hired_on_2"),
        F.nth_value("hired_on", 2).over(w_tier_by_hired_on_full).alias("second_hired_on_2"),
        F.nth_value("hired_on", 2).over(w_all_by_hired_on_full).alias("second_hired_on_3"),
        F.first("hired_on").over(w_team_by_hired_on_full).alias("first_hired_on"),
        F.row_number().over(w_hired_on_month_by_hired_on).alias("rn"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_tier_by_hired_on)).alias("days_since_prev_hired_on"),
        F.row_number().over(w_hired_on_year_tier_by_hired_3).alias("hired_on_seq"),
        F.dense_rank().over(w_hired_on_month_by_hired_on_t).alias("hired_on_dense_rank"),
    )
)

per_group = (
    ranked
    .groupBy(
        F.date_format("first_hired_on", "yyyy-MM").alias("first_hired_on_ym"),
        F.year("last_known_hired_on_2").alias("last_known_hired_on_2_year"),
    )
    .agg(
        F.count(F.when(~F.col("tier").isin("T3", "T1"), 1)).alias("n_tier_not_in_t3_t1"),
        F.max("last_known_hired_on").alias("last_last_known_hired_on"),
        F.min("rn").alias("min_rn"),
        F.avg("rn").alias("avg_rn_check"),
    )
    .filter(F.col("avg_rn_check") > 3)
    .drop("avg_rn_check")
)

result = (
    per_group
    .select(
        "first_hired_on_ym",
        "last_known_hired_on_2_year",
        "n_tier_not_in_t3_t1",
        "last_last_known_hired_on",
        "min_rn",
        F.length(F.trim(F.col("first_hired_on_ym"))).alias("first_hired_on_ym_len"),
    )
    .orderBy("first_hired_on_ym", F.col("first_hired_on_ym_len").asc())
)
