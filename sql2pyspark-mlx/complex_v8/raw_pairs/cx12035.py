from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on_year = Window.orderBy("hired_on_year", "id")
w_tier_hired_on_year_by_hired = Window.partitionBy("tier", F.year("hired_on")).orderBy("hired_on_year", "id")
w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").desc())
w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc(), "id")
w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.date_trunc("year", F.col("hired_on")).alias("hired_on_year"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_hired_on_year), 3).alias("hired_on_year_percent_rank"),
        F.lag("hired_on_year").over(w_tier_hired_on_year_by_hired).alias("prev_hired_on_year"),
        F.round((F.unix_timestamp("hired_on_year") - F.unix_timestamp(F.lag("hired_on_year").over(w_tier_hired_on_year_by_hired))) / 60, 1).alias("minutes_since_prev_hired_on_year"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on_year)).alias("days_since_prev_hired_on"),
        F.round(F.cume_dist().over(w_all_by_hired_on_year), 3).alias("hired_on_year_cume_dist"),
        F.ntile(3).over(w_all_by_hired_on).alias("hired_on_tercile"),
        F.row_number().over(w_tier_hired_on_year_by_hired).alias("hired_on_year_seq"),
        F.row_number().over(w_all_by_hired_on_year).alias("hired_on_year_seq_2"),
        F.nth_value("hired_on", 2).over(w_all_by_hired_on_full).alias("second_hired_on"),
        F.round((F.unix_timestamp("hired_on_year") - F.unix_timestamp(F.lag("hired_on_year").over(w_all_by_hired_on_year))) / 60, 1).alias("minutes_since_prev_hired_on_year_2"),
        F.round(F.percent_rank().over(w_all_by_hired_on), 3).alias("hired_on_percent_rank_2"),
    )
)

first_rows = (
    with_trends
    .filter(F.col("hired_on_year_seq") <= 5)
    .drop("hired_on_year_seq")
)

stats = (
    first_rows
    .groupBy(
        F.date_trunc("month", "prev_hired_on_year").alias("prev_hired_on_year_month"),
        F.date_format("hired_on_year", "yyyy-MM").alias("hired_on_year_ym"),
    )
    .agg(
        F.round(F.avg("hired_on_tercile"), 2).alias("avg_hired_on_tercile"),
        F.array_sort(F.collect_set("tier")).alias("tier_list"),
        F.countDistinct("tier").alias("distinct_tiers"),
        F.sum("hired_on_year_percent_rank").alias("total_hired_on_year_percent_rank"),
        F.count("*").alias("cnt"),
    )
)

result = (
    stats
    .select(
        "prev_hired_on_year_month",
        "hired_on_year_ym",
        "distinct_tiers",
        "total_hired_on_year_percent_rank",
        "cnt",
        F.when(F.col("cnt") >= 9, "large").when(F.col("cnt") >= 4, "medium").otherwise("small").alias("cnt_tier"),
        F.quarter(F.col("prev_hired_on_year_month")).alias("prev_hired_on_year_month_quarter"),
    )
)
