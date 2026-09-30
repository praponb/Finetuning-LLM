from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_premium = Window.orderBy(F.col("premium").asc_nulls_last(), "id")
w_all_by_holder_joined_on_full = Window.orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_running = Window.orderBy(F.col("premium").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_holder_joined_on_ties = Window.orderBy(F.col("holder_joined_on").desc())
w_all_by_holder_joined_on = Window.orderBy(F.col("holder_joined_on").desc(), "id")
w_premium_tercile_rn_by_premiu = Window.partitionBy("premium_tercile", "rn").orderBy("premium_share_of_status", "id")
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))

cleaned = (
    df_policies
    .filter((F.col("product").isin("TRAVEL", "LIFE", "HOME")) | (F.col("holder_id") >= 5))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.date_format(F.col("end_date"), "yyyy-MM").alias("end_date_label"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", "p.holder_name", F.col("p.joined_on").alias("holder_joined_on"))
)

windowed = (
    with_parent
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.ntile(3).over(w_all_by_premium).alias("premium_tercile"),
        F.nth_value("end_date", 2).over(w_all_by_holder_joined_on_full).alias("second_end_date"),
        F.dense_rank().over(w_all_by_premium).alias("premium_dense_rank"),
        F.min("premium").over(w_all_by_premium_running).alias("min_premium_so_far"),
        F.round(F.avg("premium").over(w_status), 2).alias("status_avg_premium"),
        F.round(F.percent_rank().over(w_all_by_holder_joined_on_ties), 3).alias("holder_joined_on_percent_rank"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.lead("end_date").over(w_all_by_holder_joined_on).alias("next_end_date"),
        F.row_number().over(w_all_by_premium).alias("rn"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.lag("end_date", 2).over(w_premium_tercile_rn_by_premiu).alias("prev2_end_date"),
        F.sum("status_avg_premium").over(w_start_date_month).alias("start_date_month_sum_status_avg_premium"),
        F.count(F.lit(1)).over(w_start_date_month).alias("rows_in_start_date_month"),
        (F.col("premium_vs_status_avg") - F.lag("premium_vs_status_avg").over(w_premium_tercile_rn_by_premiu)).alias("premium_vs_status_avg_change"),
    )
)

result = (
    sequenced
    .filter(F.col("start_date_month_sum_status_avg_premium").between(10160.0, 37550.0))
    .select(
        "id",
        "product",
        "premium",
        "end_date",
        "status",
        "end_date_label",
        "holder_name",
        "holder_joined_on",
        "rows_in_status",
        "premium_tercile",
        "premium_dense_rank",
        "min_premium_so_far",
        "status_avg_premium",
        "holder_joined_on_percent_rank",
        "premium_share_of_status",
        "premium_vs_status_avg",
        "next_end_date",
        "rn",
        "prev2_end_date",
        "start_date_month_sum_status_avg_premium",
        "rows_in_start_date_month",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("premium_tercile").desc())
)
