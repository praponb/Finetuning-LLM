from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy(F.col("end_date").asc_nulls_last())
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy("end_date", "id")
w_holder_id_by_end_date_ties_2 = Window.partitionBy("holder_id").orderBy("end_date")
w_start_date_month_status_by_h = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy(F.col("holder_joined_on").asc_nulls_last())
w_holder_id_by_end_date_2 = Window.partitionBy("holder_id").orderBy(F.col("end_date").asc_nulls_last(), "id")
w_start_date_month_status_by_h_2 = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy(F.col("holder_joined_on").asc_nulls_last(), "id")
w_end_date_rank_by_days_since = Window.partitionBy("end_date_rank").orderBy(F.col("days_since_prev_holder_joined_on").desc())
w_holder_joined_on_month_end_d = Window.partitionBy(F.date_trunc("month", "holder_joined_on"), F.month("end_date"))
w_end_date_year_status = Window.partitionBy(F.year("end_date"), "status")
w_prev2_start_date_week_month = Window.partitionBy(F.month("prev2_start_date_week")).orderBy(F.col("end_date").desc(), "id")
w_seq_no_by_days_since_prev_st = Window.partitionBy("seq_no").orderBy(F.col("days_since_prev_start_date").desc(), "id")

scoped = (
    df_policies
    .filter((F.col("premium") >= 1042.5) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
        F.date_trunc("week", F.col("start_date")).alias("start_date_week"),
    )
)

combined_details_holder = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.product_ref",
        "s.start_date_week",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

windowed = (
    combined_details_holder
    .select(
        "id",
        "end_date",
        "status",
        "product_ref",
        "holder_joined_on",
        F.rank().over(w_holder_id_by_end_date_ties).alias("end_date_rank"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_by_end_date)).alias("days_since_prev_start_date"),
        F.round(F.percent_rank().over(w_holder_id_by_end_date_ties_2), 3).alias("end_date_percent_rank"),
        F.rank().over(w_start_date_month_status_by_h).alias("holder_joined_on_rank"),
        F.row_number().over(w_holder_id_by_end_date_2).alias("seq_no"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_start_date_month_status_by_h_2)).alias("days_since_prev_holder_joined_on"),
        F.row_number().over(w_start_date_month_status_by_h_2).alias("rn"),
        F.lead("end_date").over(w_start_date_month_status_by_h_2).alias("next_end_date"),
        F.lead("start_date_week", 2).over(w_holder_id_by_end_date).alias("next2_start_date_week"),
        F.lag("start_date_week", 2).over(w_start_date_month_status_by_h_2).alias("prev2_start_date_week"),
        F.lag("start_date", 3).over(w_holder_id_by_end_date).alias("prev3_start_date"),
    )
)

top_rows = windowed.filter(F.col("rn") <= 2)

windowed_2 = (
    top_rows
    .select(
        "id",
        "end_date",
        "status",
        "product_ref",
        "holder_joined_on",
        "end_date_rank",
        "days_since_prev_start_date",
        "end_date_percent_rank",
        "holder_joined_on_rank",
        "rn",
        "next_end_date",
        "next2_start_date_week",
        "prev2_start_date_week",
        F.round(F.cume_dist().over(w_end_date_rank_by_days_since), 3).alias("days_since_prev_holder_joined_on_cume_dist"),
        F.min("seq_no").over(w_holder_joined_on_month_end_d).alias("holder_joined_on_month_end_date_month_min_seq_no"),
        F.round(F.avg("days_since_prev_start_date").over(w_holder_joined_on_month_end_d), 2).alias("holder_joined_on_month_end_date_month_avg_days_s"),
        F.lead("next2_start_date_week", 3).over(w_prev2_start_date_week_month).alias("next3_next2_start_date_week"),
        F.round((F.unix_timestamp("prev2_start_date_week") - F.unix_timestamp(F.lag("prev2_start_date_week").over(w_seq_no_by_days_since_prev_st))) / 60, 1).alias("minutes_since_prev_prev2_start_date_week"),
        F.round(F.col("days_since_prev_start_date") - F.avg("days_since_prev_start_date").over(w_holder_joined_on_month_end_d), 2).alias("days_since_prev_start_date_vs_holder_joined_on_m"),
        F.sum("days_since_prev_start_date").over(w_end_date_year_status).alias("end_date_year_status_sum_days_since_prev_start_d"),
        F.datediff(F.col("holder_joined_on"), F.lag("holder_joined_on").over(w_seq_no_by_days_since_prev_st)).alias("days_since_prev_holder_joined_on_2"),
    )
)

result = (
    windowed_2
    .filter(F.col("end_date_percent_rank") > 0.2)
    .select(
        "id",
        "end_date",
        "product_ref",
        "holder_joined_on",
        "days_since_prev_start_date",
        "end_date_percent_rank",
        "rn",
        "next_end_date",
        "next2_start_date_week",
        "prev2_start_date_week",
        "days_since_prev_holder_joined_on_cume_dist",
        "holder_joined_on_month_end_date_month_min_seq_no",
        "holder_joined_on_month_end_date_month_avg_days_s",
        "minutes_since_prev_prev2_start_date_week",
        "end_date_year_status_sum_days_since_prev_start_d",
        "days_since_prev_holder_joined_on_2",
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
    )
    .orderBy(
        F.col("next2_start_date_week").desc(),
        F.col("holder_joined_on_month_end_date_month_min_seq_no").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
