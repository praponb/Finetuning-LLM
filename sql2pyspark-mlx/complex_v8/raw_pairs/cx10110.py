from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_or_unknown = Window.partitionBy("holder_id", "product_or_unknown")
w_status_holder_id = Window.partitionBy("status", "holder_id")
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_end_date_month_by_start_date = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_by_premium_distanc = Window.partitionBy("holder_id").orderBy(F.col("premium_distance").asc_nulls_last(), "id")
w_all_by_holder_risk_score_run = Window.orderBy("holder_risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_end_date_month_by_start_date_2 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("start_date")
w_end_date_decile = Window.partitionBy("end_date_decile")
w_last_known_end_date_month_by = Window.partitionBy(F.month("last_known_end_date")).orderBy("end_date_seq", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_premium = Window.orderBy("first_known_premium", "id")
w_all_by_first_known_premium_r = Window.orderBy("first_known_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_end_date_month_by_2 = Window.partitionBy(F.month("last_known_end_date")).orderBy("end_date_seq", "id").rowsBetween(-2, Window.currentRow)

scoped_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.abs(F.col("premium") - 2289.0).alias("premium_distance"),
    )
)

enriched_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") <= 0.6)
    .select("s.*", F.col("p.risk_score").alias("holder_risk_score"))
)

with_trends = (
    enriched_holder
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "premium_distance",
        F.round(F.avg("premium_distance").over(w_holder_id_product_or_unknown), 2).alias("holder_id_product_or_unknown_avg_premium_distanc"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_holder_id_product_or_unknown), F.lit(0)), 4).alias("holder_risk_score_share_of_holder_id_product_or_"),
        F.max("holder_risk_score").over(w_status_holder_id).alias("status_holder_id_max_holder_risk_score"),
        F.row_number().over(w_all_by_end_date).alias("end_date_seq"),
        F.first("premium", ignorenulls=True).over(w_end_date_month_by_start_date).alias("first_known_premium"),
        F.row_number().over(w_holder_id_by_premium_distanc).alias("premium_distance_seq"),
        F.last("end_date", ignorenulls=True).over(w_all_by_holder_risk_score_run).alias("last_known_end_date"),
        F.first("start_date", ignorenulls=True).over(w_end_date_month_by_start_date).alias("first_known_start_date"),
        F.ntile(10).over(w_all_by_end_date).alias("end_date_decile"),
        F.dense_rank().over(w_end_date_month_by_start_date_2).alias("start_date_dense_rank"),
        F.count(F.lit(1)).over(w_status_holder_id).alias("rows_in_status_holder_id"),
        F.max("holder_risk_score").over(w_holder_id_product_or_unknown).alias("holder_id_product_or_unknown_max_holder_risk_sco"),
        F.lag("end_date").over(w_holder_id_by_premium_distanc).alias("prev_end_date"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_by_premium_distanc)).alias("premium_change"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.round(F.col("premium_distance_seq") / F.nullif(F.sum("premium_distance_seq").over(w_end_date_decile), F.lit(0)), 4).alias("premium_distance_seq_share_of_end_date_decile"),
        F.nth_value("prev_end_date", 2).over(w_last_known_end_date_month_by).alias("second_prev_end_date"),
        F.rank().over(w_all_by_first_known_premium).alias("first_known_premium_rank"),
        (F.col("end_date_decile") - F.lag("end_date_decile").over(w_all_by_first_known_premium)).alias("end_date_decile_change"),
        F.last("end_date_seq", ignorenulls=True).over(w_all_by_first_known_premium_r).alias("last_known_end_date_seq"),
        F.round(F.avg("premium_distance").over(w_last_known_end_date_month_by_2), 2).alias("premium_distance_avg_3"),
    )
)

result = (
    windowed
    .select(
        "id",
        "holder_id",
        "start_date",
        "holder_risk_score_share_of_holder_id_product_or_",
        "status_holder_id_max_holder_risk_score",
        "end_date_seq",
        "first_known_premium",
        "premium_distance_seq",
        "last_known_end_date",
        "end_date_decile",
        "start_date_dense_rank",
        "holder_id_product_or_unknown_max_holder_risk_sco",
        "prev_end_date",
        "premium_change",
        "second_prev_end_date",
        "first_known_premium_rank",
        "end_date_decile_change",
        "premium_distance_avg_3",
    )
)
