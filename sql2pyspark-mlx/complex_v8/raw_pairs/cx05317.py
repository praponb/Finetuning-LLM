from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id")
w_team_by_hired_on_ties = Window.partitionBy("team").orderBy(F.col("hired_on").desc())
w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").asc_nulls_last())
w_team_by_hired_on_running = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 4)
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.concat(F.col("agent_name"), F.lit("#"), F.col("id").cast("string")).alias("agent_name_ref"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.lead("hired_on", 2).over(w_all_by_hired_on).alias("next2_hired_on"),
        F.dense_rank().over(w_team_by_hired_on_ties).alias("hired_on_dense_rank"),
        F.lag("hired_on").over(w_all_by_hired_on).alias("prev_hired_on"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank"),
        F.first("hired_on", ignorenulls=True).over(w_team_by_hired_on_running).alias("first_known_hired_on"),
    )
)

result = (
    sequenced
    .groupBy("prev_hired_on", "next2_hired_on")
    .pivot("team", ["Billing", "Tech", "Onboarding", "VIP"])
    .agg(F.sum("hired_on_dense_rank"))
    .orderBy("prev_hired_on", "next2_hired_on")
)
