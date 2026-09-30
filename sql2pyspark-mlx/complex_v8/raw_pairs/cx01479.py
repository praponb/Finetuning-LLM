from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_capacity = Window.orderBy(F.col("capacity").asc_nulls_last(), "id")
w_city = Window.partitionBy("city")
w_all_by_capacity_ties = Window.orderBy(F.col("capacity").asc_nulls_last())

valid = (
    df_warehouses
    .filter((F.col("capacity").between(2614, 11768)) & (~F.col("city").isin("Singapore", "Kuala Lumpur")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.lag("capacity").over(w_all_by_capacity).alias("prev_capacity"),
        F.round(F.cume_dist().over(w_all_by_capacity_ties), 3).alias("capacity_cume_dist"),
        F.dense_rank().over(w_all_by_capacity_ties).alias("capacity_dense_rank"),
        F.lead("capacity").over(w_all_by_capacity).alias("next_capacity"),
    )
)

qualified = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("capacity_cume_dist").alias("overall_avg_capacity_cume_dist")))
    .filter(F.col("capacity_cume_dist") > F.col("overall_avg_capacity_cume_dist"))
    .drop("overall_avg_capacity_cume_dist")
)

result = (
    qualified
    .groupingSets(
        [["city", "capacity_dense_rank"], ["city"], []],
        "city", "capacity_dense_rank",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(~F.col("city").isin("Kuala Lumpur", "Bangkok"), F.col("prev_capacity")).otherwise(0)).alias("prev_capacity_city_not_in_kuala_lumpur_ban"),
        F.round(F.sum("capacity_dense_rank") / F.nullif(F.sum("prev_capacity"), F.lit(0)), 3).alias("capacity_dense_rank_to_prev_capacity_ratio"),
        F.sum(F.when(F.lower(F.col("wh_code")).like("%bk%"), F.col("capacity_cume_dist")).otherwise(0)).alias("capacity_cume_dist_lower_wh_code_like_bk"),
    )
    .select(
        F.coalesce(F.col("city"), F.lit("ALL")).alias("city"),
        "capacity_dense_rank",
        "grouping_level",
        "prev_capacity_city_not_in_kuala_lumpur_ban",
        "capacity_dense_rank_to_prev_capacity_ratio",
        "capacity_cume_dist_lower_wh_code_like_bk",
    )
)
