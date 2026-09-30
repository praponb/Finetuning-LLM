from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_product_by_start_date_month = Window.partitionBy("product").orderBy("start_date_month_end", "id")
w_all_by_start_date_month_end = Window.orderBy(F.col("start_date_month_end").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_group_by_start_date = Window.partitionBy("product_group").orderBy(F.col("start_date_month_end").desc(), "id")
w_product_group_by_start_date_2 = Window.partitionBy("product_group").orderBy(F.col("start_date_month_end").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_month_end_2 = Window.orderBy(F.col("start_date_month_end").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_month_end_3 = Window.orderBy(F.col("start_date_month_end").desc(), "id")
w_product_group = Window.partitionBy("product_group")
w_all_by_status_sum_holder_ris = Window.orderBy(F.col("status_sum_holder_risk_score").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_holder_id_by_status_sum_hold = Window.partitionBy("holder_id").orderBy(F.col("status_sum_holder_risk_score").desc(), "id")
w_all_by_status_sum_holder_ris_2 = Window.orderBy(F.col("status_sum_holder_risk_score").desc_nulls_first())

prepared_policys = (
    df_policies
    .filter((F.col("premium") > 2289.0) & (F.col("product") == "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("TRAVEL", "LIFE"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
    )
)

with_parent_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.product_group",
        "s.start_date_month_end",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_windows = (
    with_parent_holder
    .select(
        "*",
        F.min("holder_risk_score").over(w_status).alias("status_min_holder_risk_score"),
        (F.col("holder_risk_score") - F.lag("holder_risk_score").over(w_product_by_start_date_month)).alias("holder_risk_score_change"),
        F.sum("holder_risk_score").over(w_status).alias("status_sum_holder_risk_score"),
        F.last("holder_risk_score", ignorenulls=True).over(w_all_by_start_date_month_end).alias("last_known_holder_risk_score"),
        F.row_number().over(w_product_group_by_start_date).alias("rn"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.round(F.avg("holder_risk_score").over(w_status), 2).alias("status_avg_holder_risk_score"),
        F.size(F.collect_set("product_group").over(w_status)).alias("distinct_product_group_in_status"),
        F.last("holder_risk_score", ignorenulls=True).over(w_product_group_by_start_date_2).alias("last_known_holder_risk_score_2"),
        F.nth_value("holder_risk_score", 2).over(w_all_by_start_date_month_end_2).alias("second_holder_risk_score"),
        F.lag("holder_risk_score").over(w_all_by_start_date_month_end_3).alias("prev_holder_risk_score"),
        F.count(F.lit(1)).over(w_all_by_start_date_month_end).alias("running_count"),
        (F.col("holder_risk_score") - F.lag("holder_risk_score").over(w_product_group_by_start_date)).alias("holder_risk_score_change_2"),
        F.count(F.lit(1)).over(w_product_group).alias("rows_in_product_group"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_product_group), F.lit(0)), 4).alias("holder_risk_score_share_of_product_group"),
    )
)

annotated = (
    with_windows
    .select(
        "*",
        F.abs(F.col("holder_risk_score") - 0.6).alias("holder_risk_score_distance"),
        F.abs(F.col("rows_in_status") - 13).alias("rows_in_status_distance"),
        F.upper(F.trim(F.col("product_group"))).alias("product_group_clean"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.round(F.avg("holder_risk_score").over(w_all_by_status_sum_holder_ris), 2).alias("holder_risk_score_avg_1_prior"),
        F.lag("start_date_month_end").over(w_holder_id_by_status_sum_hold).alias("prev_start_date_month_end"),
        F.round(F.cume_dist().over(w_holder_id_by_status_sum_hold), 3).alias("status_sum_holder_risk_score_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_status_sum_holder_ris_2), 3).alias("status_sum_holder_risk_score_percent_rank"),
    )
)

result = (
    metrics
    .filter(F.col("product_group_clean").isNotNull())
    .select(
        "id",
        "product",
        "status",
        "start_date_month_end",
        "holder_risk_score",
        "status_min_holder_risk_score",
        "holder_risk_score_change",
        "status_sum_holder_risk_score",
        "last_known_holder_risk_score",
        "rn",
        "status_avg_holder_risk_score",
        "distinct_product_group_in_status",
        "last_known_holder_risk_score_2",
        "second_holder_risk_score",
        "running_count",
        "holder_risk_score_change_2",
        "rows_in_product_group",
        "product_group_clean",
        "holder_risk_score_avg_1_prior",
        "status_sum_holder_risk_score_percent_rank",
        F.date_trunc("year", F.col("start_date_month_end")).alias("start_date_month_end_year"),
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
    )
    .orderBy(F.col("distinct_product_group_in_status").desc_nulls_first(), "id")
    .limit(5)
)
