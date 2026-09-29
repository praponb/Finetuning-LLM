from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tier", "team").orderBy(F.col("hired_on_week").desc(), F.col("id").desc())

recent = (
    df_support_agents
    .filter(F.col("tier") == "T2")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.date_format(F.col("hired_on"), "yyyy-MM-dd").alias("hired_on_label"),
        F.date_trunc("week", F.col("hired_on")).alias("hired_on_week"),
    )
)

latest_per_key = (
    recent
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .cube("tier", "team")
    .agg(
        F.sum(F.when(~F.col("team").isin("Onboarding", "Billing"), 1).otherwise(0)).alias("n_team_not_in_onboarding_billi"),
        F.count("*").alias("n_rows"),
    )
    .select(
        F.coalesce(F.col("tier"), F.lit("ALL")).alias("tier"),
        F.coalesce(F.col("team"), F.lit("ALL")).alias("team"),
        "n_team_not_in_onboarding_billi",
        "n_rows",
    )
    .orderBy(F.col("tier").asc_nulls_last(), F.col("team").asc_nulls_last())
)
