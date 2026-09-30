from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region = Window.partitionBy("region")
w_region_by_risk_score_adj_ful = Window.partitionBy("region").orderBy(F.col("risk_score_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_by_risk_score_adj_tie = Window.partitionBy("region").orderBy("risk_score_adj")
w_region_by_risk_score_adj = Window.partitionBy("region").orderBy("risk_score_adj", "id")
w_all_by_risk_score_running = Window.orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_by_risk_score = Window.partitionBy("region").orderBy("risk_score", "id")
w_region_by_risk_score_ties = Window.partitionBy("region").orderBy("risk_score")
w_all_by_risk_score = Window.orderBy("risk_score", "id")
w_region_by_risk_score_adj_run = Window.partitionBy("region").orderBy(F.col("risk_score_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_policyholders
    .filter(F.year(F.col("joined_on")) == 2025)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        F.round(F.col("risk_score") * 1.07, 2).alias("risk_score_adj"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.round(F.col("risk_score_adj") / F.nullif(F.sum("risk_score_adj").over(w_region), F.lit(0)), 4).alias("risk_score_adj_share_of_region"),
        F.count(F.lit(1)).over(w_region).alias("rows_in_region"),
        F.first("risk_score_adj").over(w_region_by_risk_score_adj_ful).alias("first_risk_score_adj"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_region), F.lit(0)), 4).alias("risk_score_share_of_region"),
        F.round(F.cume_dist().over(w_region_by_risk_score_adj_tie), 3).alias("risk_score_adj_cume_dist"),
        F.row_number().over(w_region_by_risk_score_adj).alias("seq_no"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_region), 2).alias("risk_score_vs_region_avg"),
        F.last("risk_score_adj", ignorenulls=True).over(w_all_by_risk_score_running).alias("last_known_risk_score_adj"),
        F.dense_rank().over(w_region_by_risk_score).alias("risk_score_dense_rank"),
        F.round(F.cume_dist().over(w_region_by_risk_score_ties), 3).alias("risk_score_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_risk_score), 3).alias("risk_score_cume_dist_2"),
        F.min("risk_score").over(w_region_by_risk_score_adj_run).alias("min_risk_score_so_far"),
    )
)

result = (
    with_trends
    .filter(F.col("rows_in_region") < 17)
    .select(
        "id",
        "region",
        "risk_score",
        "risk_score_adj",
        "risk_score_adj_share_of_region",
        "first_risk_score_adj",
        "risk_score_adj_cume_dist",
        "seq_no",
        "risk_score_vs_region_avg",
        "last_known_risk_score_adj",
        "risk_score_dense_rank",
        "risk_score_cume_dist",
        "risk_score_cume_dist_2",
        "min_risk_score_so_far",
        F.least(F.col("first_risk_score_adj"), F.col("rows_in_region")).alias("least_first_risk_score_adj_rows_in_region"),
    )
    .orderBy(
        F.col("last_known_risk_score_adj").desc_nulls_first(),
        F.col("first_risk_score_adj").desc_nulls_first(),
    )
)
