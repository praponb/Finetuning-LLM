from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_ties = Window.orderBy("end_date")
w_all_by_end_date = Window.orderBy("end_date", "id")
w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date_running = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")

scoped_policys = (
    df_policies
    .filter(F.col("holder_id") <= 17)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(": ", F.col("status"), F.col("product")).alias("status_product_label"),
    )
)

with_windows = (
    scoped_policys
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_end_date_ties), 3).alias("end_date_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_end_date), 3).alias("end_date_cume_dist_2"),
        F.nth_value("end_date", 2).over(w_all_by_end_date_full).alias("second_end_date"),
        F.round(F.percent_rank().over(w_all_by_end_date), 3).alias("end_date_percent_rank_2"),
        F.last("start_date", ignorenulls=True).over(w_all_by_end_date_running).alias("last_known_start_date"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "status_product_label",
        "end_date_percent_rank",
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.avg("end_date_cume_dist_2").over(w_product), 2).alias("product_avg_end_date_cume_dist_2"),
        F.round(F.col("end_date_percent_rank") - F.avg("end_date_percent_rank").over(w_product), 2).alias("end_date_percent_rank_vs_product_avg"),
    )
)

result = (
    sequenced
    .groupBy("holder_id")
    .pivot("product", ["TRAVEL", "HEALTH", "AUTO", "LIFE"])
    .agg(F.max("rows_in_product"))
    .orderBy("holder_id")
)
