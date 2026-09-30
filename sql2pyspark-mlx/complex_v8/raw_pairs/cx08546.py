from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team", "tier").orderBy(F.col("days_since_hired_on").desc(), F.col("id").desc())

filtered = (
    df_support_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.date_trunc("year", F.col("hired_on")).alias("hired_on_year"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("hired_on")).alias("days_since_hired_on"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
    )
)

latest_per_key = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest = (
    latest_per_key
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .filter(~F.col("tier").isin("T3", "T1"))
    .select(
        "id",
        "team",
        "tier",
        "hired_on_year",
        "days_since_hired_on",
        "team_len",
        F.when(F.col("team").isin("VIP", "Onboarding"), "group_a").when(F.col("team") == "Tech", "group_b").otherwise("other").alias("team_group"),
        F.month(F.col("hired_on_year")).alias("hired_on_year_month"),
    )
    .orderBy("hired_on_year", F.col("hired_on_year_month").asc(), "id")
    .limit(20)
)
