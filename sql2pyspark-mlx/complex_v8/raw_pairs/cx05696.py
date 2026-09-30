from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_status = Window.partitionBy(F.year("end_date"), "status")
w_holder_id_status = Window.partitionBy("holder_id", "status")
w_status_len_product_by_status = Window.partitionBy("status_len", "product").orderBy("status_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_len_by_end_date = Window.partitionBy("status_len").orderBy("end_date", "id")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_by_start_date = Window.partitionBy("holder_id").orderBy("start_date", "id")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_len_product_by_status_2 = Window.partitionBy("status_len", "product").orderBy("status_len", "id")
w_all_by_end_date_moving_2 = Window.orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w_second_status_len = Window.partitionBy("second_status_len")
w_end_date_month_by_end_date_y = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy(F.col("end_date_year_status_sum_status_len").desc())

prepared_policys = (
    df_policies
    .filter((F.col("premium") <= 3719.7) & (F.col("holder_id") >= 13))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_parent_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.risk_score").alias("holder_risk_score"))
)

sequenced = (
    with_parent_holder
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_end_date_year_status), F.lit(0)), 4).alias("premium_share_of_end_date_year_status"),
        F.count(F.lit(1)).over(w_holder_id_status).alias("rows_in_holder_id_status"),
        F.last("holder_risk_score", ignorenulls=True).over(w_status_len_product_by_status).alias("last_known_holder_risk_score"),
        F.lead("holder_risk_score").over(w_status_len_by_end_date).alias("next_holder_risk_score"),
        F.sum("status_len").over(w_end_date_year_status).alias("end_date_year_status_sum_status_len"),
        F.size(F.collect_set("product").over(w_holder_id)).alias("distinct_product_in_holder_id"),
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
        F.lag("end_date", 3).over(w_holder_id_by_start_date).alias("prev3_end_date"),
        F.last("start_date", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_start_date"),
        F.round(F.col("status_len") / F.nullif(F.sum("status_len").over(w_holder_id_status), F.lit(0)), 4).alias("status_len_share_of_holder_id_status"),
        F.nth_value("status_len", 2).over(w_all_by_end_date_full).alias("second_status_len"),
        F.row_number().over(w_status_len_product_by_status_2).alias("rn"),
        F.sum("holder_risk_score").over(w_all_by_end_date_moving_2).alias("holder_risk_score_sum_3"),
        F.min("holder_risk_score").over(w_holder_id_status).alias("holder_id_status_min_holder_risk_score"),
    )
)

windowed = (
    sequenced
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "status_len",
        "holder_risk_score",
        "premium_share_of_end_date_year_status",
        "rows_in_holder_id_status",
        "last_known_holder_risk_score",
        "distinct_product_in_holder_id",
        "prev3_end_date",
        "last_known_start_date",
        "second_status_len",
        "rn",
        "holder_risk_score_sum_3",
        "holder_id_status_min_holder_risk_score",
        F.round(F.col("status_len") / F.nullif(F.sum("status_len").over(w_second_status_len), F.lit(0)), 4).alias("status_len_share_of_second_status_len"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_second_status_len), F.lit(0)), 4).alias("premium_share_of_second_status_len"),
        F.rank().over(w_end_date_month_by_end_date_y).alias("end_date_year_status_sum_status_len_rank"),
        F.round(F.percent_rank().over(w_end_date_month_by_end_date_y), 3).alias("end_date_year_status_sum_status_len_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "status_len",
        "premium_share_of_end_date_year_status",
        "rows_in_holder_id_status",
        "last_known_holder_risk_score",
        "distinct_product_in_holder_id",
        "prev3_end_date",
        "second_status_len",
        "rn",
        "holder_risk_score_sum_3",
        "status_len_share_of_second_status_len",
        "premium_share_of_second_status_len",
        "end_date_year_status_sum_status_len_rank",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.round(F.col("holder_risk_score_sum_3") * 1.07, 2).alias("holder_risk_score_sum_3_adj"),
    )
    .orderBy(F.col("rows_in_holder_id_status").desc_nulls_first())
)
