from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_capacity = Window.partitionBy("city").orderBy(F.col("capacity").desc(), "id")
w_city_by_capacity_distance_ru = Window.partitionBy("city").orderBy("capacity_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city = Window.partitionBy("city")
w_city_by_capacity_distance = Window.partitionBy("city").orderBy("capacity_distance", "id")
w_city_by_capacity_distance_fu = Window.partitionBy("city").orderBy("capacity_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_city_by_capacity_distance_mo = Window.partitionBy("city").orderBy(F.col("capacity_distance").desc(), "id").rowsBetween(-6, Window.currentRow)

prepared_warehouses = (
    df_warehouses
    .filter((F.col("city") == "Singapore") & (F.col("opened_on") < F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.abs(F.col("capacity") - 11506).alias("capacity_distance"),
        F.substring(F.trim(F.col("wh_code")), 1, 3).alias("wh_code_prefix"),
    )
)

sequenced = (
    prepared_warehouses
    .select(
        "*",
        F.lag("capacity", 1, 0).over(w_city_by_capacity).alias("prev_capacity"),
        F.first("capacity_distance", ignorenulls=True).over(w_city_by_capacity_distance_ru).alias("first_known_capacity_distance"),
        F.min("capacity_distance").over(w_city).alias("city_min_capacity_distance"),
        F.max("capacity").over(w_city).alias("city_max_capacity"),
        F.dense_rank().over(w_city_by_capacity).alias("capacity_dense_rank"),
        F.dense_rank().over(w_city_by_capacity_distance).alias("capacity_distance_dense_rank"),
        F.last("capacity").over(w_city_by_capacity_distance_fu).alias("last_capacity"),
        F.round(F.avg("capacity").over(w_city_by_capacity_distance_mo), 2).alias("capacity_avg_7"),
        F.round(F.avg("capacity_distance").over(w_city), 2).alias("city_avg_capacity_distance"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "capacity",
        "wh_code_prefix",
        "prev_capacity",
        "city_min_capacity_distance",
        "city_max_capacity",
        "capacity_dense_rank",
        "capacity_distance_dense_rank",
        "last_capacity",
        "capacity_avg_7",
        "city_avg_capacity_distance",
        F.concat_ws("|", F.col("wh_code"), F.col("wh_code_prefix")).alias("wh_code_wh_code_prefix_label"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code_prefix")), " ", "_")).alias("wh_code_prefix_slug"),
    )
    .orderBy(
        F.col("wh_code_prefix_slug").desc_nulls_first(),
        F.col("prev_capacity").desc(),
        "id",
    )
    .limit(5)
)
