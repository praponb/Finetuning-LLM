from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_segment_holder_id_by = Window.partitionBy("holder_segment", "holder_id").orderBy(F.col("holder_risk_score").desc_nulls_first())
w_holder_joined_on_month_start = Window.partitionBy(F.date_trunc("month", "holder_joined_on"), F.month("start_date_month_start"))
w_holder_joined_on_year_holder = Window.partitionBy(F.year("holder_joined_on"), "holder_segment").orderBy(F.col("start_date_month_start").desc(), "id")
w_holder_segment = Window.partitionBy("holder_segment")
w_holder_segment_holder_id_by_2 = Window.partitionBy("holder_segment", "holder_id").orderBy(F.col("holder_risk_score").desc_nulls_first(), "id")
w_holder_segment_holder_id_by_3 = Window.partitionBy("holder_segment", "holder_id").orderBy(F.col("holder_risk_score").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_status_holder_id = Window.partitionBy("status", "holder_id")
w_product_start_date_month_sta = Window.partitionBy("product", F.month("start_date_month_start")).orderBy("rows_in_holder_joined_on_month_start_date_month_", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_start_date_month_sta_2 = Window.partitionBy("product", F.month("start_date_month_start")).orderBy("rows_in_holder_joined_on_month_start_date_month_", "id")

recent = (
    df_policies
    .filter((F.col("end_date").isNotNull()) & (F.col("product") == "TRAVEL"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.start_date_month_start",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.dense_rank().over(w_holder_segment_holder_id_by).alias("holder_risk_score_dense_rank"),
        F.min("holder_risk_score").over(w_holder_joined_on_month_start).alias("holder_joined_on_month_start_date_month_start_mo"),
        F.row_number().over(w_holder_joined_on_year_holder).alias("seq_no"),
        F.count(F.lit(1)).over(w_holder_joined_on_month_start).alias("rows_in_holder_joined_on_month_start_date_month_"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_segment), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_segment"),
        F.count(F.lit(1)).over(w_holder_segment).alias("rows_in_holder_segment"),
        F.dense_rank().over(w_holder_segment_holder_id_by_2).alias("holder_risk_score_dense_rank_2"),
        F.round(F.avg("holder_risk_score").over(w_holder_segment_holder_id_by_3), 2).alias("holder_risk_score_avg_3"),
        (F.col("holder_risk_score") - F.lag("holder_risk_score").over(w_holder_joined_on_year_holder)).alias("holder_risk_score_change"),
        F.lag("holder_risk_score", 2).over(w_holder_segment_holder_id_by_2).alias("prev2_holder_risk_score"),
    )
)

labeled = (
    with_trends
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "start_date_month_start",
        "holder_risk_score",
        "holder_segment",
        "holder_risk_score_dense_rank",
        "holder_joined_on_month_start_date_month_start_mo",
        "seq_no",
        "rows_in_holder_joined_on_month_start_date_month_",
        "holder_risk_score_share_of_holder_segment",
        "rows_in_holder_segment",
        "holder_risk_score_dense_rank_2",
        "holder_risk_score_avg_3",
        "holder_risk_score_change",
        "prev2_holder_risk_score",
        F.round(F.col("prev2_holder_risk_score") * 1.1, 2).alias("prev2_holder_risk_score_adj"),
    )
)

with_windows = (
    labeled
    .select(
        "id",
        "product",
        "start_date",
        "holder_segment",
        "holder_risk_score_dense_rank",
        "holder_joined_on_month_start_date_month_start_mo",
        "seq_no",
        "holder_risk_score_share_of_holder_segment",
        "rows_in_holder_segment",
        "holder_risk_score_dense_rank_2",
        "holder_risk_score_change",
        "prev2_holder_risk_score",
        F.count(F.lit(1)).over(w_status_holder_id).alias("rows_in_status_holder_id"),
        F.nth_value("start_date", 2).over(w_product_start_date_month_sta).alias("second_start_date"),
        F.size(F.collect_set("product").over(w_status_holder_id)).alias("distinct_product_in_status_holder_id"),
        F.lag("rows_in_holder_segment", 3).over(w_product_start_date_month_sta_2).alias("prev3_rows_in_holder_segment"),
        F.max("holder_risk_score").over(w_status_holder_id).alias("status_holder_id_max_holder_risk_score"),
        F.sum("holder_risk_score_change").over(w_status_holder_id).alias("status_holder_id_sum_holder_risk_score_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "start_date",
        "holder_segment",
        "holder_risk_score_dense_rank",
        "holder_joined_on_month_start_date_month_start_mo",
        "seq_no",
        "holder_risk_score_share_of_holder_segment",
        "rows_in_holder_segment",
        "prev2_holder_risk_score",
        "rows_in_status_holder_id",
        "second_start_date",
        "distinct_product_in_status_holder_id",
        "prev3_rows_in_holder_segment",
        "status_holder_id_max_holder_risk_score",
        F.datediff(F.col("second_start_date"), F.col("start_date")).alias("days_start_date_to_second_start_date"),
    )
    .orderBy("prev2_holder_risk_score", "id")
    .limit(10)
)
