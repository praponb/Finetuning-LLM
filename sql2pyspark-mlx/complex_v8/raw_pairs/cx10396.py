from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_by_kwh_adj_running = Window.partitionBy("site_id").orderBy(F.col("kwh_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_full_2 = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_running_2 = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_meter_readings
    .filter(F.col("voltage").between(221.8, 233.6))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.sum("kwh_adj").over(w_site_id_by_kwh_adj_running).alias("running_kwh_adj"),
        F.ntile(3).over(w_all_by_kwh).alias("kwh_tercile"),
        F.nth_value("voltage", 2).over(w_all_by_kwh_full).alias("second_voltage"),
        F.last("kwh_adj").over(w_all_by_kwh_full_2).alias("last_kwh_adj"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.first("kwh_adj", ignorenulls=True).over(w_all_by_kwh_running).alias("first_known_kwh_adj"),
        F.first("kwh", ignorenulls=True).over(w_all_by_kwh_running_2).alias("first_known_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.rank().over(w_all_by_kwh).alias("kwh_rank_2"),
    )
)

top_per_group = ranked.filter(F.col("kwh_rank") <= 5)

result = (
    top_per_group
    .select(
        "id",
        "kwh_adj",
        "rows_in_site_id",
        "running_kwh_adj",
        "kwh_tercile",
        "second_voltage",
        "last_kwh_adj",
        "kwh_rank",
        "first_known_kwh_adj",
        "first_known_kwh",
        "kwh_rank_2",
    )
)
