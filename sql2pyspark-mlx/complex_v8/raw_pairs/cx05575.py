from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_full = Window.orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_or_unknown = Window.partitionBy("status_or_unknown")
w_all_by_start_date_ties = Window.orderBy(F.col("start_date").asc_nulls_last())
w_end_date_year = Window.partitionBy(F.year("end_date"))
w_start_date_month_by_start_da = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("start_date", "id")
w_start_date_month_by_start_da_2 = Window.partitionBy(F.date_trunc("month", "start_date")).orderBy("start_date")

scoped_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.date_trunc("month", F.col("start_date")).alias("start_date_month"),
    )
)

enriched = (
    scoped_policys
    .filter(F.col("status_or_unknown").isin("LAPSED", "CANCELLED", "EXPIRED"))
    .select("*", F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"))
)

metrics = (
    enriched
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "status_or_unknown",
        "start_date_month",
        F.first("premium").over(w_all_by_start_date_full).alias("first_premium"),
        F.size(F.collect_set("holder_id").over(w_status_or_unknown)).alias("distinct_holder_id_in_status_or_unknown"),
        F.round(F.cume_dist().over(w_all_by_start_date_ties), 3).alias("start_date_cume_dist"),
        F.sum("premium").over(w_status_or_unknown).alias("status_or_unknown_sum_premium"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "start_date",
        "status",
        "status_or_unknown",
        "start_date_month",
        "first_premium",
        "distinct_holder_id_in_status_or_unknown",
        "start_date_cume_dist",
        F.round(F.col("first_premium") - F.avg("first_premium").over(w_end_date_year), 2).alias("first_premium_vs_end_date_year_avg"),
        F.lag("first_premium").over(w_start_date_month_by_start_da).alias("prev_first_premium"),
        F.round(F.cume_dist().over(w_start_date_month_by_start_da_2), 3).alias("start_date_cume_dist_2"),
        F.count(F.lit(1)).over(w_end_date_year).alias("rows_in_end_date_year"),
        F.lead("start_date_month").over(w_start_date_month_by_start_da).alias("next_start_date_month"),
        F.sum("start_date_cume_dist").over(w_end_date_year).alias("end_date_year_sum_start_date_cume_dist"),
    )
)

enriched_2 = (
    metrics_2
    .select(
        "*",
        F.abs(F.col("prev_first_premium") - 2319.8).alias("prev_first_premium_distance"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown_2"),
        F.least(F.col("rows_in_end_date_year"), F.col("prev_first_premium")).alias("least_rows_in_end_date_year_prev_first_premium"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "start_date",
        "status_or_unknown",
        "start_date_month",
        "first_premium",
        "distinct_holder_id_in_status_or_unknown",
        "start_date_cume_dist",
        "first_premium_vs_end_date_year_avg",
        "prev_first_premium",
        "start_date_cume_dist_2",
        "rows_in_end_date_year",
        "next_start_date_month",
        "prev_first_premium_distance",
        "status_or_unknown_2",
    )
    .orderBy(F.col("start_date_month").desc(), F.col("start_date").asc())
)
