from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_dayofweek = Window.orderBy(F.col("start_date_dayofweek").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_dayofweek_2 = Window.orderBy(F.col("start_date_dayofweek").asc_nulls_last())
w_all_by_start_date_dayofweek_3 = Window.orderBy(F.col("start_date_dayofweek").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_dayofweek_4 = Window.orderBy(F.col("start_date_dayofweek").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_holder_id_end_date_month = Window.partitionBy("holder_id", F.date_trunc("month", "end_date"))
w_status_by_start_date_dayofwe = Window.partitionBy("status").orderBy(F.col("start_date_dayofweek_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_premium = Window.orderBy("second_premium", "id")

valid_policys = (
    df_policies
    .filter((F.col("status") == "EXPIRED") & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.dayofweek(F.col("start_date")).alias("start_date_dayofweek"),
    )
)

with_windows = (
    valid_policys
    .select(
        "*",
        F.nth_value("premium", 2).over(w_all_by_start_date_dayofweek).alias("second_premium"),
        F.round(F.cume_dist().over(w_all_by_start_date_dayofweek_2), 3).alias("start_date_dayofweek_cume_dist"),
        F.first("start_date_dayofweek", ignorenulls=True).over(w_all_by_start_date_dayofweek_3).alias("first_known_start_date_dayofweek"),
        F.round(F.avg("premium").over(w_all_by_start_date_dayofweek_4), 2).alias("premium_avg_2"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        "start_date_dayofweek",
        "second_premium",
        "start_date_dayofweek_cume_dist",
        "first_known_start_date_dayofweek",
        "premium_avg_2",
        F.round(F.col("start_date_dayofweek") - F.avg("start_date_dayofweek").over(w_holder_id_end_date_month), 2).alias("start_date_dayofweek_vs_holder_id_end_date_month"),
        F.last("premium", ignorenulls=True).over(w_status_by_start_date_dayofwe).alias("last_known_premium"),
        F.count(F.lit(1)).over(w_holder_id_end_date_month).alias("rows_in_holder_id_end_date_month"),
        F.rank().over(w_all_by_second_premium).alias("second_premium_rank"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "second_premium_rank",
        "rows_in_holder_id_end_date_month",
        "first_known_start_date_dayofweek",
        "start_date_dayofweek",
    )
    .unpivot(
        ids=["id"],
        values=["second_premium_rank", "rows_in_holder_id_end_date_month", "first_known_start_date_dayofweek", "start_date_dayofweek"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
