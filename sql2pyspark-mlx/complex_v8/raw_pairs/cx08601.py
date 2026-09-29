from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_by_hired_on = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id")
w_team_by_hired_on_full = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_support_agents
    .filter((F.col("tier") == "T1") & (~F.col("team").isin("Onboarding", "Billing")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
    )
)

enriched = (
    recent
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        "team_ref",
        F.upper(F.trim(F.col("team"))).alias("team_clean"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "team",
        "hired_on",
        F.row_number().over(w_team_by_hired_on).alias("seq_no"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_team_by_hired_on)).alias("days_since_prev_hired_on"),
        F.nth_value("hired_on", 2).over(w_team_by_hired_on_full).alias("second_hired_on"),
        F.ntile(5).over(w_team_by_hired_on).alias("hired_on_quintile"),
    )
)

result = (
    windowed
    .rollup("team", "hired_on_quintile", "seq_no")
    .agg(
        F.sum("seq_no").alias("total_seq_no"),
        F.min("second_hired_on").alias("first_second_hired_on"),
    )
)
