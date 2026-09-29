from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_status = Window.partitionBy("holder_id", "status")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_status_by_end_date = Window.partitionBy("holder_id", "status").orderBy("end_date", "id")
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date = Window.orderBy(F.col("end_date").asc_nulls_last(), "id")
w_all_by_end_date_moving_1 = Window.orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_premium_distance = Window.orderBy("premium_distance", "id")
w_end_date_dense_rank_last_kno = Window.partitionBy("end_date_dense_rank", F.date_trunc("month", "last_known_end_date"))
w_all_by_prev3_end_date_ties = Window.orderBy("prev3_end_date")
w_prev3_end_date_month_by_rows = Window.partitionBy(F.month("prev3_end_date")).orderBy("rows_in_holder_id", "id").rowsBetween(-1, Window.currentRow)

recent = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "LAPSED", "CANCELLED")) & (F.col("start_date") < F.lit("2025-05-02").cast("date")))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 2844.1).alias("premium_distance"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.premium_distance",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.rank().over(w_holder_id_status_by_end_date).alias("end_date_rank"),
        F.last("end_date", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_end_date"),
        F.lag("end_date", 3).over(w_all_by_end_date).alias("prev3_end_date"),
        F.round(F.avg("premium_distance").over(w_all_by_end_date_moving_1), 2).alias("premium_distance_avg_2"),
        F.count(F.lit(1)).over(w_holder_id_status).alias("rows_in_holder_id_status"),
        F.lead("premium_distance", 1, 0).over(w_all_by_premium_distance).alias("next_premium_distance"),
        F.dense_rank().over(w_all_by_end_date).alias("end_date_dense_rank"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "end_date",
        "status",
        "holder_risk_score",
        "rows_in_holder_id",
        "end_date_rank",
        "last_known_end_date",
        "premium_distance_avg_2",
        "rows_in_holder_id_status",
        F.round(F.col("end_date_rank") - F.avg("end_date_rank").over(w_end_date_dense_rank_last_kno), 2).alias("end_date_rank_vs_end_date_dense_rank_last_known_"),
        F.round(F.col("next_premium_distance") / F.nullif(F.sum("next_premium_distance").over(w_end_date_dense_rank_last_kno), F.lit(0)), 4).alias("next_premium_distance_share_of_end_date_dense_ra"),
        F.rank().over(w_all_by_prev3_end_date_ties).alias("prev3_end_date_rank"),
        F.size(F.collect_set("status").over(w_end_date_dense_rank_last_kno)).alias("distinct_status_in_end_date_dense_rank_last_know"),
        F.sum("next_premium_distance").over(w_end_date_dense_rank_last_kno).alias("end_date_dense_rank_last_known_end_date_month_su"),
        F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_end_date_dense_rank_last_kno), 2).alias("premium_distance_vs_end_date_dense_rank_last_kno"),
        F.max("next_premium_distance").over(w_prev3_end_date_month_by_rows).alias("next_premium_distance_max_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "end_date",
        "holder_risk_score",
        "rows_in_holder_id",
        "end_date_rank",
        "last_known_end_date",
        "next_premium_distance_share_of_end_date_dense_ra",
        "premium_distance_vs_end_date_dense_rank_last_kno",
        F.round(F.col("premium_distance_avg_2") / F.nullif(F.col("rows_in_holder_id"), F.lit(0)), 3).alias("premium_distance_avg_2_per_rows_in_holder_id"),
        F.date_format(F.col("last_known_end_date"), "yyyy-MM-dd").alias("last_known_end_date_label"),
    )
    .orderBy(F.col("end_date_rank").desc_nulls_first())
)
