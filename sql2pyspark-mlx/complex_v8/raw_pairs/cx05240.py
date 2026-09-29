from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_hired_on_plus_30d_dayof = Window.partitionBy("tier", "hired_on_plus_30d_dayofweek").orderBy(F.col("hired_on_plus_30d_dayofweek").desc(), "id")
w_hired_on_plus_30d_dayofweek = Window.partitionBy("hired_on_plus_30d_dayofweek").orderBy("hired_on_plus_30d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tier_hired_on_plus_30d_dayof_2 = Window.partitionBy("tier", "hired_on_plus_30d_dayofweek").orderBy(F.col("hired_on_plus_30d_dayofweek").desc())

valid_agents = (
    df_support_agents
    .filter((F.col("team").isNotNull()) & (F.col("hired_on") < F.lit("2025-03-30").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.date_add(F.col("hired_on"), 30).alias("hired_on_plus_30d"),
    )
)

scored = (
    valid_agents
    .select(
        "id",
        "team",
        "tier",
        "hired_on_plus_30d",
        F.quarter(F.col("hired_on_plus_30d")).alias("hired_on_plus_30d_quarter"),
        F.date_add(F.col("hired_on_plus_30d"), 7).alias("hired_on_plus_30d_plus_7d"),
    )
)

annotated = scored.withColumn("team_slug", F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")))

tagged = (
    annotated
    .select(
        "*",
        F.dayofweek(F.col("hired_on_plus_30d")).alias("hired_on_plus_30d_dayofweek"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.lead("hired_on_plus_30d").over(w_tier_hired_on_plus_30d_dayof).alias("next_hired_on_plus_30d"),
        F.sum("hired_on_plus_30d_quarter").over(w_hired_on_plus_30d_dayofweek).alias("running_hired_on_plus_30d_quarter"),
        F.dense_rank().over(w_tier_hired_on_plus_30d_dayof_2).alias("hired_on_plus_30d_dayofweek_dense_rank"),
    )
)

leaders = (
    sequenced
    .filter((F.col("hired_on_plus_30d_dayofweek_dense_rank") == 1) & (F.col("team").isin("Onboarding", "Tech")))
    .drop("hired_on_plus_30d_dayofweek_dense_rank")
)

screened = (
    leaders
    .crossJoin(leaders.agg(F.min("hired_on_plus_30d_dayofweek").alias("overall_min_hired_on_plus_30d_dayofweek")))
    .filter(F.col("hired_on_plus_30d_dayofweek") > F.col("overall_min_hired_on_plus_30d_dayofweek"))
    .drop("overall_min_hired_on_plus_30d_dayofweek")
)

result = (
    screened
    .select("id", "hired_on_plus_30d_quarter", "hired_on_plus_30d_dayofweek")
    .unpivot(
        ids=["id"],
        values=["hired_on_plus_30d_quarter", "hired_on_plus_30d_dayofweek"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
)
