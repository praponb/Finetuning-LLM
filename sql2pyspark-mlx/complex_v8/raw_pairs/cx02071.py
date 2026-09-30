from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_opened_on_month_by_capa = Window.partitionBy("city", F.month("opened_on")).orderBy("capacity", "id")
w_opened_on_week_month = Window.partitionBy(F.month("opened_on_week"))
w_all_by_opened_on_week_ties = Window.orderBy("opened_on_week")
w_opened_on_month_city_by_capa = Window.partitionBy(F.month("opened_on"), "city").orderBy(F.col("capacity").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_on_month_city_by_capa_2 = Window.partitionBy(F.month("opened_on"), "city").orderBy(F.col("capacity").asc_nulls_last(), "id")
w_all_by_opened_on_week = Window.orderBy("opened_on_week", "id")

base = (
    df_warehouses
    .filter(F.col("capacity") < 11768)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_trunc("week", F.col("opened_on")).alias("opened_on_week"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.lead("opened_on_week", 2).over(w_city_opened_on_month_by_capa).alias("next2_opened_on_week"),
        F.sum("capacity").over(w_opened_on_week_month).alias("opened_on_week_month_sum_capacity"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_opened_on_week_month), F.lit(0)), 4).alias("capacity_share_of_opened_on_week_month"),
        F.count(F.lit(1)).over(w_opened_on_week_month).alias("rows_in_opened_on_week_month"),
        F.dense_rank().over(w_all_by_opened_on_week_ties).alias("opened_on_week_dense_rank"),
        F.first("capacity").over(w_opened_on_month_city_by_capa).alias("first_capacity"),
        F.dense_rank().over(w_opened_on_month_city_by_capa_2).alias("capacity_dense_rank"),
        F.ntile(4).over(w_all_by_opened_on_week).alias("opened_on_week_quartile"),
        F.round(F.avg("capacity").over(w_city), 2).alias("city_avg_capacity"),
        F.ntile(5).over(w_all_by_opened_on_week).alias("opened_on_week_quintile"),
        F.rank().over(w_all_by_opened_on_week).alias("opened_on_week_rank"),
        F.lead("opened_on", 2).over(w_city_opened_on_month_by_capa).alias("next2_opened_on"),
        F.nth_value("capacity", 2).over(w_opened_on_month_city_by_capa).alias("second_capacity"),
    )
)

per_group = (
    sequenced
    .groupBy("opened_on_week_quintile")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("opened_on_week_quintile").alias("max_opened_on_week_quintile"),
        F.round(F.avg("opened_on_week_month_sum_capacity"), 2).alias("avg_opened_on_week_month_sum_capacity"),
        F.sum("opened_on_week_dense_rank").alias("total_opened_on_week_dense_rank"),
        F.sum("second_capacity").alias("sum_second_capacity_check"),
    )
    .filter(F.col("sum_second_capacity_check") > 2614)
    .drop("sum_second_capacity_check")
)

result = (
    per_group
    .select(
        "opened_on_week_quintile",
        "distinct_ids",
        "max_opened_on_week_quintile",
        "total_opened_on_week_dense_rank",
        F.round(F.col("opened_on_week_quintile") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("opened_on_week_quintile_per_distinct_ids"),
    )
    .orderBy("distinct_ids")
)
