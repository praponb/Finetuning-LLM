from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_risk_score = Window.orderBy(F.col("risk_score").asc_nulls_last(), "id")
w_all_by_risk_score_moving_1 = Window.orderBy("risk_score", "id").rowsBetween(-1, Window.currentRow)
w_segment = Window.partitionBy("segment")
w_segment_by_risk_score = Window.partitionBy("segment").orderBy("risk_score", "id")
w_all_by_risk_score_2 = Window.orderBy("risk_score", "id")
w_all_by_risk_score_full = Window.orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_risk_score_seq_by_avg_rows_i = Window.partitionBy("risk_score_seq").orderBy(F.col("avg_rows_in_segment").desc_nulls_first(), "seq_no", "risk_score_seq")
w_seq_no_by_pct_segment_not_in = Window.partitionBy("seq_no").orderBy("pct_segment_not_in_vip_retail")
w_risk_score_seq_seq_no = Window.partitionBy("risk_score_seq", "seq_no")

prepared_holders = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-07-18").cast("date")) & (F.col("region").isin("South", "North", "East")))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "segment",
        F.lower(F.regexp_replace(F.trim(F.col("region")), " ", "_")).alias("region_slug"),
    )
)

with_trends = (
    prepared_holders
    .select(
        "id",
        "holder_name",
        "risk_score",
        "segment",
        "region_slug",
        F.lag("risk_score").over(w_all_by_risk_score).alias("prev_risk_score"),
        F.round(F.avg("risk_score").over(w_all_by_risk_score_moving_1), 2).alias("risk_score_avg_2"),
        F.max("risk_score").over(w_segment).alias("segment_max_risk_score"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_segment), F.lit(0)), 4).alias("risk_score_share_of_segment"),
        (F.col("risk_score") - F.lag("risk_score").over(w_segment_by_risk_score)).alias("risk_score_change"),
        F.row_number().over(w_segment_by_risk_score).alias("seq_no"),
        F.sum("risk_score").over(w_segment).alias("segment_sum_risk_score"),
        F.row_number().over(w_all_by_risk_score_2).alias("risk_score_seq"),
        (F.col("risk_score") - F.lag("risk_score").over(w_all_by_risk_score)).alias("risk_score_change_2"),
        F.count(F.lit(1)).over(w_segment).alias("rows_in_segment"),
        F.nth_value("risk_score", 2).over(w_all_by_risk_score_full).alias("second_risk_score"),
        F.ntile(4).over(w_segment_by_risk_score).alias("risk_score_quartile"),
        F.min("risk_score").over(w_segment).alias("segment_min_risk_score"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_segment), 2).alias("risk_score_vs_segment_avg"),
    )
)

summary = (
    with_trends
    .filter(F.col("risk_score_change_2") <= 0.5)
    .groupBy("seq_no", "risk_score_seq")
    .agg(
        F.sum(F.when(F.col("segment") == "VIP", F.col("risk_score_change"))).alias("risk_score_change_segment_vip"),
        F.round(F.avg("rows_in_segment"), 2).alias("avg_rows_in_segment"),
        F.round(100 * F.count(F.when(~F.col("segment").isin("VIP", "RETAIL"), 1)) / F.count("*"), 1).alias("pct_segment_not_in_vip_retail"),
        F.array_sort(F.collect_set("holder_name")).alias("holder_name_list"),
        F.avg("rows_in_segment").alias("avg_rows_in_segment_check"),
    )
    .filter(F.col("avg_rows_in_segment_check") > 8)
    .drop("avg_rows_in_segment_check")
)

ranked = (
    summary
    .select(
        "*",
        F.row_number().over(w_risk_score_seq_by_avg_rows_i).alias("seq_no_2"),
        F.rank().over(w_seq_no_by_pct_segment_not_in).alias("pct_segment_not_in_vip_retail_rank"),
        F.round(F.col("avg_rows_in_segment") - F.avg("avg_rows_in_segment").over(w_risk_score_seq_seq_no), 2).alias("avg_rows_in_segment_vs_risk_score_seq_seq_no_avg"),
    )
)

result = (
    ranked
    .select(
        "seq_no",
        "risk_score_seq",
        "risk_score_change_segment_vip",
        "pct_segment_not_in_vip_retail",
        "holder_name_list",
        "seq_no_2",
        "pct_segment_not_in_vip_retail_rank",
        F.when(F.col("risk_score_seq") >= 7, "large").when(F.col("risk_score_seq") >= 5, "medium").otherwise("small").alias("risk_score_seq_tier"),
    )
    .orderBy(
        F.col("seq_no_2").desc(),
        F.col("pct_segment_not_in_vip_retail_rank").desc_nulls_first(),
    )
)
