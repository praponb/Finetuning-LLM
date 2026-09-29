from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_hired_on_ties = Window.partitionBy("team").orderBy("hired_on")
w_hired_on_month_by_hired_on_f = Window.partitionBy(F.month("hired_on")).orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_support_agents
    .filter(~F.col("tier").isin("T3", "T1"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

ranked = (
    prepared
    .withColumn("hired_on_cume_dist", F.round(F.cume_dist().over(w_team_by_hired_on_ties), 3))
    .withColumn("second_hired_on", F.nth_value("hired_on", 2).over(w_hired_on_month_by_hired_on_f))
)

without_matches = (
    ranked
    .crossJoin(ranked.agg(F.max("hired_on_cume_dist").alias("overall_max_hired_on_cume_dist")))
    .filter(F.col("hired_on_cume_dist") < F.col("overall_max_hired_on_cume_dist"))
    .drop("overall_max_hired_on_cume_dist")
)

screened = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("hired_on_cume_dist").alias("overall_avg_hired_on_cume_dist")))
    .filter(F.col("hired_on_cume_dist") > F.col("overall_avg_hired_on_cume_dist"))
    .drop("overall_avg_hired_on_cume_dist")
)

result = (
    screened
    .groupingSets(
        [["team", "tier"], ["team"], ["tier"]],
        "team", "tier",
    )
    .agg(
        F.grouping("team").alias("is_total_team"),
        F.count("*").alias("row_count"),
        F.min("hired_on").alias("first_hired_on"),
    )
    .orderBy(F.col("team").asc_nulls_last(), F.col("tier").asc_nulls_last())
)
