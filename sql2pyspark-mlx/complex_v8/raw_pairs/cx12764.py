from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_holder_id_by_start_da = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc(), "id")
w_status_holder_id_by_start_da_2 = Window.partitionBy("status", "holder_id").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_by_end_date_m = Window.partitionBy("holder_segment").orderBy(F.col("end_date_month_start").desc(), "id")
w_start_date_month_holder_segm = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_segment")
w_holder_segment_by_end_date_m_2 = Window.partitionBy("holder_segment").orderBy(F.col("end_date_month_start").desc(), "id").rowsBetween(-2, -1)
w_holder_id_status_by_holder_r = Window.partitionBy("holder_id", "status").orderBy("holder_risk_score", "id")
w_holder_segment_by_end_date_m_3 = Window.partitionBy("holder_segment").orderBy(F.col("end_date_month_start").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_moving_1 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").desc())
w_rn = Window.partitionBy("rn")
w_all_by_start_date_month_hold = Window.orderBy(F.col("start_date_month_holder_segment_avg_premium").desc_nulls_first(), "id")

prepared = (
    df_policies
    .filter((~F.col("product").isin("AUTO", "LIFE")) & (F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2026-09-23").cast("date"))))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.trunc(F.col("end_date"), "MM").alias("end_date_month_start"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "premium",
        "start_date",
        "product_ref",
        "holder_region",
        "holder_segment",
        F.row_number().over(w_status_holder_id_by_start_da).alias("rn"),
        F.first("premium", ignorenulls=True).over(w_status_holder_id_by_start_da_2).alias("first_known_premium"),
        F.lag("holder_risk_score").over(w_holder_segment_by_end_date_m).alias("prev_holder_risk_score"),
        F.min("premium").over(w_start_date_month_holder_segm).alias("start_date_month_holder_segment_min_premium"),
        F.sum("holder_risk_score").over(w_start_date_month_holder_segm).alias("start_date_month_holder_segment_sum_holder_risk_"),
        F.sum("holder_risk_score").over(w_holder_segment_by_end_date_m_2).alias("holder_risk_score_sum_2_prior"),
        F.row_number().over(w_holder_segment_by_end_date_m).alias("end_date_month_start_seq"),
        F.round(F.percent_rank().over(w_holder_id_status_by_holder_r), 3).alias("holder_risk_score_percent_rank"),
        F.count(F.lit(1)).over(w_holder_segment_by_end_date_m_3).alias("running_count"),
        F.round(F.avg("premium").over(w_start_date_month_holder_segm), 2).alias("start_date_month_holder_segment_avg_premium"),
        F.dense_rank().over(w_status_holder_id_by_start_da).alias("start_date_dense_rank"),
        F.count(F.lit(1)).over(w_start_date_month_holder_segm).alias("rows_in_start_date_month_holder_segment"),
    )
)

ranked = (
    with_trends
    .select(
        "id",
        "premium",
        "start_date",
        "product_ref",
        "holder_region",
        "holder_segment",
        "rn",
        "prev_holder_risk_score",
        "start_date_month_holder_segment_min_premium",
        "start_date_month_holder_segment_sum_holder_risk_",
        "holder_risk_score_sum_2_prior",
        "holder_risk_score_percent_rank",
        "start_date_month_holder_segment_avg_premium",
        "start_date_dense_rank",
        "rows_in_start_date_month_holder_segment",
        F.round(F.avg("premium").over(w_all_by_start_date_moving_1), 2).alias("premium_avg_2"),
        F.rank().over(w_all_by_start_date_ties).alias("start_date_rank"),
        F.round(F.avg("premium").over(w_rn), 2).alias("rn_avg_premium"),
        (F.col("first_known_premium") - F.lag("first_known_premium").over(w_all_by_start_date_month_hold)).alias("first_known_premium_change"),
        F.sum("running_count").over(w_rn).alias("rn_sum_running_count"),
        F.min("first_known_premium").over(w_rn).alias("rn_min_first_known_premium"),
    )
)

result = (
    ranked
    .filter(F.col("holder_segment") == "RETAIL")
    .select(
        "id",
        "premium",
        "start_date",
        "product_ref",
        "holder_region",
        "holder_segment",
        "prev_holder_risk_score",
        "start_date_month_holder_segment_min_premium",
        "start_date_month_holder_segment_sum_holder_risk_",
        "holder_risk_score_sum_2_prior",
        "start_date_month_holder_segment_avg_premium",
        "start_date_dense_rank",
        "rows_in_start_date_month_holder_segment",
        "premium_avg_2",
        "start_date_rank",
        "rn_avg_premium",
        "rn_min_first_known_premium",
    )
    .orderBy(F.col("holder_segment").desc(), "id")
    .limit(10)
)
