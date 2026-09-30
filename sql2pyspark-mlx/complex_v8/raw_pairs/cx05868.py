from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_region_by_risk_score = Window.partitionBy("segment", "region").orderBy("risk_score", "id")
w_segment_region_by_risk_score_2 = Window.partitionBy("segment", "region").orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_segment_region_by_risk_score_3 = Window.partitionBy("segment", "region").orderBy("risk_score", "id").rowsBetween(-1, -1)
w_all_by_risk_score_ties = Window.orderBy(F.col("risk_score").desc())
w_segment_region = Window.partitionBy("segment", "region")
w_all_by_risk_score = Window.orderBy(F.col("risk_score").desc(), "id")
w_segment_region_by_risk_score_4 = Window.partitionBy("segment", "region").orderBy("risk_score", "id").rowsBetween(-2, -1)

scoped_holders = df_policyholders.filter(F.col("joined_on") >= F.lit("2024-10-23").cast("date"))

with_activity = (
    scoped_holders
    .crossJoin(scoped_holders.agg(F.max("risk_score").alias("overall_max_risk_score")))
    .filter(F.col("risk_score") < F.col("overall_max_risk_score"))
    .drop("overall_max_risk_score")
)

windowed = (
    with_activity
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        F.lead("joined_on").over(w_segment_region_by_risk_score).alias("next_joined_on"),
        F.nth_value("joined_on", 2).over(w_segment_region_by_risk_score_2).alias("second_joined_on"),
        F.round(F.avg("risk_score").over(w_segment_region_by_risk_score_3), 2).alias("risk_score_avg_1_prior"),
        F.round(F.cume_dist().over(w_all_by_risk_score_ties), 3).alias("risk_score_cume_dist"),
        F.lag("joined_on", 3).over(w_all_by_risk_score).alias("prev3_joined_on"),
        F.max("risk_score").over(w_segment_region_by_risk_score_4).alias("risk_score_max_2_prior"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

with_stats = (
    windowed.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

filtered = (
    with_stats.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.year(F.col("p.start_date")) == 2025) & (F.col("p.product") == "TRAVEL"), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "region",
        "risk_score",
        "next_joined_on",
        "second_joined_on",
        "risk_score_avg_1_prior",
        "risk_score_cume_dist",
        "risk_score_max_2_prior",
        F.when(F.col("region").isin("West", "North"), "group_a").when(F.col("region") == "South", "group_b").otherwise("other").alias("region_group"),
        F.date_trunc("quarter", F.col("next_joined_on")).alias("next_joined_on_quarter"),
    )
)
