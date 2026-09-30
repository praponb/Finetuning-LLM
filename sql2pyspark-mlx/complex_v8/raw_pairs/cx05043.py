from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_or_unknown_by_end_dat = Window.partitionBy("status_or_unknown").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc_nulls_first(), "id")
w_status_or_unknown_by_end_dat_2 = Window.partitionBy("status_or_unknown").orderBy(F.col("end_date").desc(), "id")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_by_premium_prior_6 = Window.partitionBy("holder_id").orderBy("premium", "id").rowsBetween(-6, -1)
w_all_by_end_date_ties = Window.orderBy(F.col("end_date").desc_nulls_first())
w_status_or_unknown_product_by = Window.partitionBy("status_or_unknown", "product").orderBy("premium_distance", "id")
w_product = Window.partitionBy("product")
w_holder_id_status_or_unknown = Window.partitionBy("holder_id", "status_or_unknown").orderBy(F.col("premium").desc(), "id")
w_status_or_unknown_by_end_dat_3 = Window.partitionBy("status_or_unknown").orderBy(F.col("end_date").desc())

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") <= 13) & (F.col("status").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.abs(F.col("premium") - 2319.8).alias("premium_distance"),
    )
)

with_windows = (
    filtered_policys
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "status_or_unknown",
        "premium_distance",
        F.first("premium", ignorenulls=True).over(w_holder_id_by_premium_running).alias("first_known_premium"),
        F.nth_value("premium", 2).over(w_status_or_unknown_by_end_dat).alias("second_premium"),
        F.lead("end_date", 3).over(w_all_by_end_date).alias("next3_end_date"),
        F.row_number().over(w_status_or_unknown_by_end_dat_2).alias("seq_no"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_holder_id), F.lit(0)), 4).alias("premium_distance_share_of_holder_id"),
        F.dense_rank().over(w_all_by_end_date).alias("end_date_dense_rank"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_all_by_end_date)).alias("days_since_prev_end_date"),
        F.round(F.avg("premium_distance").over(w_holder_id_by_premium_prior_6), 2).alias("premium_distance_avg_6_prior"),
        F.rank().over(w_all_by_end_date_ties).alias("end_date_rank"),
        F.round(F.percent_rank().over(w_status_or_unknown_product_by), 3).alias("premium_distance_percent_rank"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.row_number().over(w_holder_id_status_or_unknown).alias("rn"),
        F.round(F.cume_dist().over(w_status_or_unknown_by_end_dat_3), 3).alias("end_date_cume_dist"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_product), F.lit(0)), 4).alias("premium_distance_share_of_product"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product",
        "end_date",
        "status_or_unknown",
        "premium_distance",
        "second_premium",
        "premium_distance_share_of_holder_id",
        "end_date_dense_rank",
        "days_since_prev_end_date",
        "end_date_rank",
        "premium_distance_percent_rank",
        "product_avg_premium",
        "rn",
        "premium_distance_share_of_product",
        F.coalesce(F.col("end_date_dense_rank"), F.lit(0)).alias("end_date_dense_rank_filled"),
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
    )
    .orderBy(F.col("second_premium").desc_nulls_first())
)
