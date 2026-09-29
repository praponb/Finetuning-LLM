from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_holder_risk_score_mov = Window.orderBy("holder_risk_score", "id").rowsBetween(-6, Window.currentRow)
w_all_by_holder_risk_score_tie = Window.orderBy("holder_risk_score")
w_start_date_month_status_by_h = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_holder_risk_score = Window.orderBy("holder_risk_score", "id")
w_start_date_month_status_by_h_2 = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_risk_score_ful = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_month = Window.partitionBy(F.month("end_date"))
w_start_date_month_status_by_h_3 = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy("holder_risk_score")
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc())
w_product_end_date_year_by_end = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("end_date_month_avg_holder_risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_end_date_month = Window.partitionBy("holder_id", F.month("end_date"))

valid = (
    df_policies
    .filter(F.col("product") == "LIFE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("product"), F.col("status")).alias("product_status_label"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_status_label",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.max("premium").over(w_all_by_holder_risk_score_mov).alias("premium_max_7"),
        F.dense_rank().over(w_all_by_holder_risk_score_tie).alias("holder_risk_score_dense_rank"),
        F.nth_value("premium", 2).over(w_start_date_month_status_by_h).alias("second_premium"),
        F.ntile(10).over(w_all_by_holder_risk_score).alias("holder_risk_score_decile"),
        F.sum("holder_risk_score").over(w_start_date_month_status_by_h_2).alias("running_holder_risk_score"),
        F.min("holder_risk_score").over(w_all_by_start_date_running).alias("min_holder_risk_score_so_far"),
        F.nth_value("start_date", 2).over(w_all_by_holder_risk_score_ful).alias("second_start_date"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_end_date_month), F.lit(0)), 4).alias("holder_risk_score_share_of_end_date_month"),
        F.round(F.percent_rank().over(w_start_date_month_status_by_h_3), 3).alias("holder_risk_score_percent_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_end_date_year), F.lit(0)), 4).alias("premium_share_of_end_date_year"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_end_date_month), 2).alias("holder_risk_score_vs_end_date_month_avg"),
        F.round(F.avg("holder_risk_score").over(w_end_date_month), 2).alias("end_date_month_avg_holder_risk_score"),
        F.count(F.lit(1)).over(w_end_date_month).alias("rows_in_end_date_month"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "holder_risk_score",
        "premium_max_7",
        "holder_risk_score_dense_rank",
        "second_premium",
        "holder_risk_score_decile",
        "running_holder_risk_score",
        "min_holder_risk_score_so_far",
        "second_start_date",
        "holder_risk_score_share_of_end_date_month",
        "holder_risk_score_vs_end_date_month_avg",
        "end_date_month_avg_holder_risk_score",
        "rows_in_end_date_month",
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.last("holder_risk_score").over(w_product_end_date_year_by_end).alias("last_holder_risk_score"),
        F.round(F.col("min_holder_risk_score_so_far") - F.avg("min_holder_risk_score_so_far").over(w_holder_id_end_date_month), 2).alias("min_holder_risk_score_so_far_vs_holder_id_end_da"),
        F.count(F.lit(1)).over(w_holder_id_end_date_month).alias("rows_in_holder_id_end_date_month"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        "holder_risk_score",
        "premium_max_7",
        "holder_risk_score_decile",
        "min_holder_risk_score_so_far",
        "second_start_date",
        "holder_risk_score_share_of_end_date_month",
        "holder_risk_score_vs_end_date_month_avg",
        "rows_in_end_date_month",
        "end_date_cume_dist",
        "min_holder_risk_score_so_far_vs_holder_id_end_da",
        "rows_in_holder_id_end_date_month",
        F.initcap(F.trim(F.col("product"))).alias("product_clean"),
    )
)
