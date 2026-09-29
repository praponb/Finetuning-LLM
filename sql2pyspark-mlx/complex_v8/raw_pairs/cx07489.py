from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt = Window.partitionBy("cnt")

cleaned = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.substring(F.trim(F.col("team")), 1, 3).alias("team_prefix"),
        F.upper(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean"),
        F.lower(F.trim(F.col("adjuster_name"))).alias("adjuster_name_clean_2"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

profiled = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        "c.last_claim_reported_at",
        "c.first_claim_date",
    )
)

summary = (
    profiled
    .filter(F.col("level") < 3)
    .groupBy(
        "level",
        F.date_trunc("month", "last_claim_reported_at").alias("last_claim_reported_at_month"),
        "team",
    )
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.max("last_claim_reported_at").alias("last_last_claim_reported_at"),
        F.count("*").alias("cnt"),
        F.round(F.avg("level"), 2).alias("avg_level"),
        F.sum(F.when(F.col("team").isNotNull(), F.col("level"))).alias("level_team_is_not_null"),
    )
    .filter(F.col("cnt") >= 2)
)

with_windows = (
    summary
    .withColumn("avg_level_vs_cnt_avg", F.round(F.col("avg_level") - F.avg("avg_level").over(w_cnt), 2))
    .withColumn("cnt_vs_cnt_avg", F.round(F.col("cnt") - F.avg("cnt").over(w_cnt), 2))
)

derived = with_windows.select("*", F.initcap(F.trim(F.col("team"))).alias("team_clean"))

annotated = (
    derived
    .filter(F.col("level_team_is_not_null") <= 14)
    .select(
        "*",
        F.when(F.col("team").isin("Motor", "Health"), "group_a").when(F.col("team") == "Travel", "group_b").otherwise("other").alias("team_group"),
        F.round(F.col("avg_level_vs_cnt_avg") / F.nullif(F.col("avg_level"), F.lit(0)), 3).alias("avg_level_vs_cnt_avg_per_avg_level"),
    )
)

with_activity = (
    annotated
    .crossJoin(annotated.agg(F.avg("cnt_vs_cnt_avg").alias("overall_avg_cnt_vs_cnt_avg")))
    .filter(F.col("cnt_vs_cnt_avg") > F.col("overall_avg_cnt_vs_cnt_avg"))
    .drop("overall_avg_cnt_vs_cnt_avg")
)

result = (
    with_activity
    .select(
        "level",
        "last_claim_reported_at_month",
        "avg_level_vs_cnt_avg_per_avg_level",
        "avg_level",
        "avg_level_vs_cnt_avg",
        "cnt_vs_cnt_avg",
    )
    .unpivot(
        ids=["level", "last_claim_reported_at_month"],
        values=["avg_level_vs_cnt_avg_per_avg_level", "avg_level", "avg_level_vs_cnt_avg", "cnt_vs_cnt_avg"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("level", "last_claim_reported_at_month", "metric")
)
