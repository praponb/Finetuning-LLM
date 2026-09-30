from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_dayofweek = Window.partitionBy("hired_on_dayofweek")
w_hired_on_month_hired_on_dayo = Window.partitionBy(F.month("hired_on"), "hired_on_dayofweek").orderBy("hired_on", "id")
w_all_by_hired_on_dayofweek = Window.orderBy(F.col("hired_on_dayofweek").asc_nulls_last(), "id")
w_hired_on_month_hired_on_dayo_2 = Window.partitionBy(F.month("hired_on"), "hired_on_dayofweek").orderBy("hired_on", "id").rowsBetween(-1, -1)
w_all_by_hired_on_dayofweek_ti = Window.orderBy(F.col("hired_on_dayofweek").asc_nulls_last())
w_all_by_hired_on_dayofweek_pr = Window.orderBy(F.col("hired_on_dayofweek").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_hired_on_month_team_by_hired = Window.partitionBy(F.month("hired_on"), "team").orderBy("hired_on_dayofweek", "id")
w_all_by_hired_on_dayofweek_su = Window.orderBy("hired_on_dayofweek_sum_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on_dayofweek_su_2 = Window.orderBy("hired_on_dayofweek_sum_2_prior", "id")
w_hired_on_dayofweek_by_prev_h = Window.partitionBy("hired_on_dayofweek").orderBy("prev_hired_on_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_support_agents
    .filter(F.col("hired_on") >= F.lit("2024-05-02").cast("date"))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.dayofweek(F.col("hired_on")).alias("hired_on_dayofweek"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.col("hired_on_dayofweek") - F.avg("hired_on_dayofweek").over(w_hired_on_dayofweek), 2).alias("hired_on_dayofweek_vs_hired_on_dayofweek_avg"),
        F.lag("hired_on_dayofweek").over(w_hired_on_month_hired_on_dayo).alias("prev_hired_on_dayofweek"),
        F.ntile(3).over(w_all_by_hired_on_dayofweek).alias("hired_on_dayofweek_tercile"),
        F.max("hired_on_dayofweek").over(w_hired_on_month_hired_on_dayo_2).alias("hired_on_dayofweek_max_1_prior"),
        F.dense_rank().over(w_all_by_hired_on_dayofweek_ti).alias("hired_on_dayofweek_dense_rank"),
        F.sum("hired_on_dayofweek").over(w_all_by_hired_on_dayofweek_pr).alias("hired_on_dayofweek_sum_2_prior"),
        F.count(F.lit(1)).over(w_hired_on_dayofweek).alias("rows_in_hired_on_dayofweek"),
        F.lag("hired_on").over(w_hired_on_month_team_by_hired).alias("prev_hired_on"),
        F.round(F.cume_dist().over(w_hired_on_month_team_by_hired), 3).alias("hired_on_dayofweek_cume_dist"),
    )
)

leaders = (
    with_windows
    .filter(F.col("hired_on_dayofweek_dense_rank") <= 2)
    .drop("hired_on_dayofweek_dense_rank")
)

sequenced = (
    leaders
    .select(
        "*",
        F.last("hired_on", ignorenulls=True).over(w_all_by_hired_on_dayofweek_su).alias("last_known_hired_on"),
        (F.col("hired_on_dayofweek_cume_dist") - F.lag("hired_on_dayofweek_cume_dist").over(w_all_by_hired_on_dayofweek_su_2)).alias("hired_on_dayofweek_cume_dist_change"),
        F.max("hired_on_dayofweek").over(w_hired_on_dayofweek_by_prev_h).alias("max_hired_on_dayofweek_so_far"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_name",
        "hired_on_dayofweek_vs_hired_on_dayofweek_avg",
        "prev_hired_on_dayofweek",
        "hired_on_dayofweek_max_1_prior",
        "rows_in_hired_on_dayofweek",
        "prev_hired_on",
        "hired_on_dayofweek_cume_dist",
        "last_known_hired_on",
        "hired_on_dayofweek_cume_dist_change",
        "max_hired_on_dayofweek_so_far",
        (F.col("prev_hired_on_dayofweek") - F.col("hired_on_dayofweek")).alias("prev_hired_on_dayofweek_minus_hired_on_dayofweek"),
        F.datediff(F.col("prev_hired_on"), F.col("last_known_hired_on")).alias("days_last_known_hired_on_to_prev_hired_on"),
    )
)
