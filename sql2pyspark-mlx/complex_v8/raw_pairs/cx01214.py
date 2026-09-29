from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_risk_score_ties = Window.orderBy(F.col("risk_score").desc())
w_segment = Window.partitionBy("segment")

base_holders = (
    df_policyholders
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.concat_ws("|", F.col("region"), F.col("segment")).alias("region_segment_label"),
        F.concat(F.col("region"), F.lit("#"), F.col("id").cast("string")).alias("region_ref"),
    )
)

ranked = (
    base_holders
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_risk_score_ties), 3).alias("risk_score_percent_rank"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_segment), 2).alias("risk_score_vs_segment_avg"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.status").isNotNull()), "left_anti")
)

screened = (
    with_activity
    .crossJoin(with_activity.agg(F.min("risk_score").alias("overall_min_risk_score")))
    .filter(F.col("risk_score") > F.col("overall_min_risk_score"))
    .drop("overall_min_risk_score")
)

result = (
    screened
    .groupBy("joined_on")
    .pivot("segment", ["SME", "RETAIL", "VIP"])
    .agg(F.avg("risk_score"))
    .orderBy("joined_on")
)
