from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id")
w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").asc_nulls_last())
w_all_by_hired_on_moving_1 = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_hired_on_dayofweek = Window.partitionBy("hired_on_dayofweek")

base = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2024) & (F.col("tier").isin("T1", "T2")))
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.dayofweek(F.col("hired_on")).alias("hired_on_dayofweek"),
    )
)

enriched = (
    base
    .filter(F.length(F.trim(F.col("agent_name"))) >= 8)
    .select(
        "*",
        F.when(F.col("tier").isin("T2", "T1"), "group_a").when(F.col("tier") == "T3", "group_b").otherwise("other").alias("tier_group"),
        F.upper(F.trim(F.col("tier"))).alias("tier_clean"),
        F.date_format(F.col("hired_on"), "yyyy-MM-dd").alias("hired_on_label"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.rank().over(w_all_by_hired_on).alias("hired_on_rank"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank"),
        (F.col("hired_on_dayofweek") - F.lag("hired_on_dayofweek").over(w_all_by_hired_on)).alias("hired_on_dayofweek_change"),
        F.max("hired_on_dayofweek").over(w_all_by_hired_on_moving_1).alias("hired_on_dayofweek_max_2"),
        F.row_number().over(w_all_by_hired_on).alias("rn"),
        F.size(F.collect_set("tier_group").over(w_hired_on_dayofweek)).alias("distinct_tier_group_in_hired_on_dayofweek"),
    )
)

result = (
    with_trends
    .groupBy("rn", "hired_on_rank")
    .pivot("tier_group", ["group_b", "group_a"])
    .agg(F.max("hired_on_percent_rank").alias("max_v"), F.sum("hired_on_percent_rank").alias("sum_v"))
)
