from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_end_date_year_by_e = Window.partitionBy("holder_id", F.year("end_date")).orderBy("end_date", "id")
w_holder_id_end_date_year_by_e_2 = Window.partitionBy("holder_id", F.year("end_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date_ties = Window.partitionBy("status").orderBy(F.col("end_date").desc())
w_status = Window.partitionBy("status")
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_end_date_year_by_e_3 = Window.partitionBy("holder_id", F.year("end_date")).orderBy("end_date")
w_end_date_dense_rank = Window.partitionBy("end_date_dense_rank")
w_end_date_dense_rank_2_by_end = Window.partitionBy("end_date_dense_rank_2").orderBy(F.col("end_date_cume_dist").desc(), "id")
w_end_date_dense_rank_2_end_da = Window.partitionBy("end_date_dense_rank_2", "end_date_dense_rank")
w_end_date_dense_rank_2_by_sec = Window.partitionBy("end_date_dense_rank_2").orderBy("second_premium", "id")
w_end_date_dense_rank_by_end_d = Window.partitionBy("end_date_dense_rank").orderBy(F.col("end_date_percent_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
    )
)

ranked = (
    base_policys
    .select(
        "id",
        "holder_id",
        "end_date",
        "status",
        F.dense_rank().over(w_holder_id_end_date_year_by_e).alias("end_date_dense_rank"),
        F.nth_value("premium", 2).over(w_holder_id_end_date_year_by_e_2).alias("second_premium"),
        F.round(F.percent_rank().over(w_status_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.round(F.percent_rank().over(w_holder_id_end_date_year_by_e), 3).alias("end_date_percent_rank_2"),
        F.size(F.collect_set("holder_id").over(w_status)).alias("distinct_holder_id_in_status"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.min("premium").over(w_status).alias("status_min_premium"),
        F.round(F.cume_dist().over(w_status_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.dense_rank().over(w_holder_id_end_date_year_by_e_3).alias("end_date_dense_rank_2"),
    )
)

derived = (
    ranked
    .filter(F.col("end_date_cume_dist") < 0.7)
    .select(
        "id",
        "status",
        "end_date_dense_rank",
        "second_premium",
        "end_date_percent_rank_2",
        "status_min_premium",
        "end_date_cume_dist",
        "end_date_dense_rank_2",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.greatest(F.col("end_date_percent_rank_2"), F.col("end_date_dense_rank_2")).alias("greatest_end_date_percent_rank_2_end_date_dense_"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.min("end_date_percent_rank_2").over(w_end_date_dense_rank).alias("end_date_dense_rank_min_end_date_percent_rank_2"),
        F.round(F.cume_dist().over(w_end_date_dense_rank_2_by_end), 3).alias("end_date_cume_dist_cume_dist"),
        F.round(F.avg("end_date_percent_rank_2").over(w_end_date_dense_rank_2_end_da), 2).alias("end_date_dense_rank_2_end_date_dense_rank_avg_en"),
        F.round(F.avg("end_date_dense_rank").over(w_end_date_dense_rank_2_end_da), 2).alias("end_date_dense_rank_2_end_date_dense_rank_avg_en_2"),
        F.lead("days_since_end_date", 1, 0).over(w_end_date_dense_rank_2_by_sec).alias("next_days_since_end_date"),
        (F.col("days_since_end_date") - F.lag("days_since_end_date").over(w_end_date_dense_rank_2_by_end)).alias("days_since_end_date_change"),
        F.last("end_date_cume_dist", ignorenulls=True).over(w_end_date_dense_rank_by_end_d).alias("last_known_end_date_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "status",
        "end_date_percent_rank_2",
        "status_min_premium",
        "end_date_cume_dist",
        "end_date_dense_rank_2",
        "days_since_end_date",
        "end_date_dense_rank_min_end_date_percent_rank_2",
        "end_date_cume_dist_cume_dist",
        "end_date_dense_rank_2_end_date_dense_rank_avg_en",
        "end_date_dense_rank_2_end_date_dense_rank_avg_en_2",
        "next_days_since_end_date",
        "days_since_end_date_change",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.round(F.col("end_date_dense_rank") / F.nullif(F.col("end_date_dense_rank_2"), F.lit(0)), 3).alias("end_date_dense_rank_per_end_date_dense_rank_2"),
    )
    .orderBy(F.col("end_date_percent_rank_2").desc())
)
