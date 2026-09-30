from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_region_group_by_risk = Window.partitionBy("region", "region_group").orderBy(F.col("risk_score").desc(), "id")
w_region_len_region_group = Window.partitionBy("region_len", "region_group")
w_region_len_by_risk_score = Window.partitionBy("region_len").orderBy(F.col("risk_score").desc(), "id")
w_region_region_group_by_risk_2 = Window.partitionBy("region", "region_group").orderBy(F.col("risk_score").desc())
w_all_by_rows_in_region_len_re = Window.orderBy("rows_in_region_len_region_group", "id")
w_region_region_group = Window.partitionBy("region", "region_group")
w_all_by_rows_in_region_len_re_2 = Window.orderBy("rows_in_region_len_region_group")
w_region_segment_label_len_by = Window.partitionBy("region_segment_label_len").orderBy(F.col("rows_in_region_len_region_group_cume_dist").desc(), "id")
w_all_by_region_len_change_vs = Window.orderBy("region_len_change_vs_region_region_group_avg", "id")
w_region_segment_label_len_by_2 = Window.partitionBy("region_segment_label_len").orderBy(F.col("rows_in_region_len_region_group_cume_dist").desc())
w_region_len_risk_score_decile = Window.partitionBy("region_len", "risk_score_decile")

scoped = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("M%")) & (F.col("region") == "North"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.length(F.trim(F.col("region"))).alias("region_len"),
        F.when(F.col("region").isin("North", "East"), "group_a").when(F.col("region") == "West", "group_b").otherwise("other").alias("region_group"),
        F.concat_ws("-", F.col("region"), F.col("segment")).alias("region_segment_label"),
    )
)

windowed = (
    scoped
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        "region_len",
        "region_group",
        "region_segment_label",
        (F.col("risk_score") - F.lag("risk_score").over(w_region_region_group_by_risk)).alias("risk_score_change"),
        F.ntile(10).over(w_region_region_group_by_risk).alias("risk_score_decile"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_region_len_region_group), F.lit(0)), 4).alias("risk_score_share_of_region_len_region_group"),
        F.count(F.lit(1)).over(w_region_len_region_group).alias("rows_in_region_len_region_group"),
        (F.col("region_len") - F.lag("region_len").over(w_region_len_by_risk_score)).alias("region_len_change"),
        F.rank().over(w_region_region_group_by_risk_2).alias("risk_score_rank"),
    )
)

leaders = (
    windowed
    .filter((F.col("risk_score_rank") <= 2) & (F.col("rows_in_region_len_region_group") <= 17))
    .drop("risk_score_rank")
)

metrics = (
    leaders
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_rows_in_region_len_re), 3).alias("rows_in_region_len_region_group_cume_dist"),
        F.round(F.col("region_len_change") - F.avg("region_len_change").over(w_region_region_group), 2).alias("region_len_change_vs_region_region_group_avg"),
        F.dense_rank().over(w_all_by_rows_in_region_len_re_2).alias("rows_in_region_len_region_group_dense_rank"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.least(F.col("risk_score_decile"), F.col("region_len")).alias("least_risk_score_decile_region_len"),
        F.length(F.trim(F.col("region_segment_label"))).alias("region_segment_label_len"),
        F.substring(F.trim(F.col("region_group")), 1, 4).alias("region_group_prefix"),
    )
)

with_activity = (
    derived.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.status").isin("LAPSED", "EXPIRED")), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        "region_group",
        "region_segment_label",
        "risk_score_change",
        "risk_score_decile",
        "rows_in_region_len_region_group",
        "region_len_change",
        "rows_in_region_len_region_group_cume_dist",
        "rows_in_region_len_region_group_dense_rank",
        "least_risk_score_decile_region_len",
        "region_segment_label_len",
        "region_group_prefix",
        (F.col("region_len_change") - F.lag("region_len_change").over(w_region_segment_label_len_by)).alias("region_len_change_change"),
        F.lag("risk_score", 1, 0).over(w_all_by_region_len_change_vs).alias("prev_risk_score"),
        F.round(F.percent_rank().over(w_region_segment_label_len_by_2), 3).alias("rows_in_region_len_region_group_cume_dist_percen"),
        F.round(F.col("region_len_change") - F.avg("region_len_change").over(w_region_len_risk_score_decile), 2).alias("region_len_change_vs_region_len_risk_score_decil"),
        F.lead("region_segment_label_len", 3).over(w_region_segment_label_len_by).alias("next3_region_segment_label_len"),
        F.lead("region_segment_label_len").over(w_region_segment_label_len_by).alias("next_region_segment_label_len"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        "region_group",
        "region_segment_label",
        "risk_score_change",
        "risk_score_decile",
        "rows_in_region_len_region_group",
        "region_len_change",
        "rows_in_region_len_region_group_cume_dist",
        "region_segment_label_len",
        "region_group_prefix",
        "region_len_change_change",
        "prev_risk_score",
        "rows_in_region_len_region_group_cume_dist_percen",
        "region_len_change_vs_region_len_risk_score_decil",
        "next3_region_segment_label_len",
        F.when(F.col("region_group").isin("other", "group_a"), "group_a").when(F.col("region_group") == "group_b", "group_b").otherwise("other").alias("region_group_group"),
        F.least(F.col("risk_score"), F.col("risk_score_decile")).alias("least_risk_score_risk_score_decile"),
    )
    .orderBy("risk_score_change")
)
