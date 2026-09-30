from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_risk_score = Window.orderBy("risk_score", "id")
w_region_segment_group = Window.partitionBy("region", "segment_group")
w_all_by_risk_score_ties = Window.orderBy("risk_score")
w_all_by_risk_score_running = Window.orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_policyholders
    .filter(F.col("segment") == "RETAIL")
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        F.when(F.col("segment").isin("RETAIL", "SME"), "group_a").when(F.col("segment") == "VIP", "group_b").otherwise("other").alias("segment_group"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.rank().over(w_all_by_risk_score).alias("risk_score_rank"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_region_segment_group), F.lit(0)), 4).alias("risk_score_share_of_region_segment_group"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_region_segment_group), 2).alias("risk_score_vs_region_segment_group_avg"),
        F.dense_rank().over(w_all_by_risk_score_ties).alias("risk_score_dense_rank"),
        F.sum("risk_score").over(w_all_by_risk_score_running).alias("running_risk_score"),
    )
)

leaders = sequenced.filter(F.col("risk_score_rank") <= 5)

result = (
    leaders
    .groupingSets(
        [["risk_score_dense_rank", "risk_score_rank"], ["risk_score_dense_rank"], []],
        "risk_score_dense_rank", "risk_score_rank",
    )
    .agg(F.count("*").alias("row_count"))
)
