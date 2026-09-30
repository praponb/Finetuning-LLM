from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_month_by_start_da = Window.partitionBy(F.month("start_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_start_date_month_by_start_da_2 = Window.partitionBy(F.month("start_date")).orderBy("start_date")
w_start_date_month_by_start_da_3 = Window.partitionBy(F.month("start_date")).orderBy("start_date", "id")
w_start_date_month_by_start_da_4 = Window.partitionBy(F.month("start_date")).orderBy("start_date", "id").rowsBetween(-6, -1)

cleaned_policys = (
    df_policies
    .filter(F.col("status") == "EXPIRED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

with_windows = (
    cleaned_policys
    .select(
        "*",
        F.min("premium").over(w_start_date_month_by_start_da).alias("min_premium_so_far"),
        F.round(F.percent_rank().over(w_start_date_month_by_start_da_2), 3).alias("start_date_percent_rank"),
        F.round(F.cume_dist().over(w_start_date_month_by_start_da_2), 3).alias("start_date_cume_dist"),
        F.round(F.percent_rank().over(w_start_date_month_by_start_da_3), 3).alias("start_date_percent_rank_2"),
        F.row_number().over(w_start_date_month_by_start_da_3).alias("rn"),
        F.sum("premium").over(w_start_date_month_by_start_da_4).alias("premium_sum_6_prior"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["holder_id", "status"], ["holder_id"], ["status"]],
        "holder_id", "status",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("status").asc_nulls_last())
)
