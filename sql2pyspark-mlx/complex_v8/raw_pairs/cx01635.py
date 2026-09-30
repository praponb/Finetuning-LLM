from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_all_by_capacity_distance = Window.orderBy(F.col("capacity_distance").asc_nulls_last(), "id")
w_city_by_capacity_running = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_capacity = Window.partitionBy("city").orderBy("capacity", "id")
w_city_by_capacity_full = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_city_by_capacity_distance = Window.partitionBy("city").orderBy(F.col("capacity_distance").desc(), "id")
w_all_by_capacity_distance_2 = Window.orderBy("capacity_distance", "id")
w_all_by_seq_no_ties = Window.orderBy(F.col("seq_no").desc())
w_seq_no_city_by_next_capacity = Window.partitionBy("seq_no", "city").orderBy(F.col("next_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_capacity_quintile = Window.partitionBy("seq_no", "capacity_quintile")
w_seq_no_city_by_next_capacity_2 = Window.partitionBy("seq_no", "city").orderBy(F.col("next_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_next_capacity_ties = Window.partitionBy("city").orderBy(F.col("next_capacity").asc_nulls_last())
w_all_by_seq_no_moving_3 = Window.orderBy(F.col("seq_no").desc(), "id").rowsBetween(-3, Window.currentRow)

cleaned_warehouses = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.abs(F.col("capacity") - 11506).alias("capacity_distance"),
        F.substring(F.trim(F.col("city")), 1, 2).alias("city_prefix"),
    )
)

sequenced = (
    cleaned_warehouses
    .select(
        "*",
        F.count(F.lit(1)).over(w_city).alias("rows_in_city"),
        F.row_number().over(w_all_by_capacity_distance).alias("capacity_distance_seq"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_city), 2).alias("capacity_vs_city_avg"),
        F.first("capacity_distance", ignorenulls=True).over(w_city_by_capacity_running).alias("first_known_capacity_distance"),
        F.round(F.cume_dist().over(w_city_by_capacity), 3).alias("capacity_cume_dist"),
        F.ntile(5).over(w_city_by_capacity).alias("capacity_quintile"),
        F.lead("capacity").over(w_city_by_capacity).alias("next_capacity"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_city), F.lit(0)), 4).alias("capacity_share_of_city"),
        (F.col("capacity_distance") - F.lag("capacity_distance").over(w_all_by_capacity_distance)).alias("capacity_distance_change"),
        F.round(F.col("capacity_distance") - F.avg("capacity_distance").over(w_city), 2).alias("capacity_distance_vs_city_avg"),
        F.nth_value("capacity", 2).over(w_city_by_capacity_full).alias("second_capacity"),
        F.row_number().over(w_city_by_capacity_distance).alias("seq_no"),
        F.max("capacity_distance").over(w_city).alias("city_max_capacity_distance"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "capacity",
        "city_prefix",
        "rows_in_city",
        "capacity_cume_dist",
        "capacity_quintile",
        "next_capacity",
        "capacity_share_of_city",
        "capacity_distance_change",
        "capacity_distance_vs_city_avg",
        "second_capacity",
        "seq_no",
        "city_max_capacity_distance",
        F.lead("capacity_share_of_city", 1, 0).over(w_all_by_capacity_distance_2).alias("next_capacity_share_of_city"),
        F.round(F.cume_dist().over(w_all_by_seq_no_ties), 3).alias("seq_no_cume_dist"),
        F.nth_value("capacity", 2).over(w_seq_no_city_by_next_capacity).alias("second_capacity_2"),
        F.count(F.lit(1)).over(w_seq_no_capacity_quintile).alias("rows_in_seq_no_capacity_quintile"),
        F.count(F.lit(1)).over(w_seq_no_city_by_next_capacity_2).alias("running_count"),
        F.dense_rank().over(w_city_by_next_capacity_ties).alias("next_capacity_dense_rank"),
        F.round(F.avg("capacity_distance_change").over(w_all_by_seq_no_moving_3), 2).alias("capacity_distance_change_avg_4"),
    )
)

grouped = (
    sequenced_2
    .groupBy(
        F.when(F.col("seq_no_cume_dist") >= 0.7, "A").when(F.col("seq_no_cume_dist") >= 0.3, "B").otherwise("C").alias("seq_no_cume_dist_tier"),
    )
    .agg(
        F.round(F.stddev("capacity_share_of_city"), 2).alias("stddev_capacity_share_of_city"),
        F.sum("capacity_distance_change").alias("total_capacity_distance_change"),
        F.min("city_max_capacity_distance").alias("min_city_max_capacity_distance"),
        F.round(F.sum("capacity_cume_dist"), 2).alias("total_capacity_cume_dist"),
        F.count("*").alias("cnt"),
        F.max("capacity_quintile").alias("max_capacity_quintile_check"),
    )
    .filter(F.col("max_capacity_quintile_check") > 4)
    .drop("max_capacity_quintile_check")
)

result = (
    grouped
    .select(
        "seq_no_cume_dist_tier",
        "stddev_capacity_share_of_city",
        "total_capacity_cume_dist",
        F.abs(F.col("cnt") - 9).alias("cnt_distance"),
    )
    .orderBy("seq_no_cume_dist_tier")
)
