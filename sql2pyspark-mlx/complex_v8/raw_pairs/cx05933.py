from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_opened_on_year_city = Window.partitionBy(F.year("opened_on"), "city")
w_city_by_opened_on_running = Window.partitionBy("city").orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_opened_on_moving_1 = Window.partitionBy("city").orderBy("opened_on", "id").rowsBetween(-1, Window.currentRow)
w_city_by_opened_on = Window.partitionBy("city").orderBy("opened_on", "id")
w_city_by_opened_on_ties = Window.partitionBy("city").orderBy("opened_on")

filtered_warehouses = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
    )
)

ranked = (
    filtered_warehouses
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        "wh_code_slug",
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_city), F.lit(0)), 4).alias("capacity_share_of_city"),
        F.count(F.lit(1)).over(w_opened_on_year_city).alias("rows_in_opened_on_year_city"),
        F.last("opened_on", ignorenulls=True).over(w_city_by_opened_on_running).alias("last_known_opened_on"),
        F.sum("capacity").over(w_city_by_opened_on_running).alias("running_capacity"),
        F.sum("capacity").over(w_city_by_opened_on_moving_1).alias("capacity_sum_2"),
        F.count(F.lit(1)).over(w_city).alias("rows_in_city"),
        (F.col("capacity") - F.lag("capacity").over(w_city_by_opened_on)).alias("capacity_change"),
        F.sum("capacity").over(w_opened_on_year_city).alias("opened_on_year_city_sum_capacity"),
        F.dense_rank().over(w_city_by_opened_on_ties).alias("opened_on_dense_rank"),
        F.max("capacity").over(w_opened_on_year_city).alias("opened_on_year_city_max_capacity"),
    )
)

per_group = (
    ranked
    .groupBy(F.year("opened_on").alias("opened_on_year"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.upper(F.trim(F.col("wh_code"))).like("B%"), 1)).alias("n_upper_trim_wh_code_like_b"),
        F.max("capacity_sum_2").alias("max_capacity_sum_2_check"),
    )
    .filter(F.col("max_capacity_sum_2_check") > 6900)
    .drop("max_capacity_sum_2_check")
)

result = (
    per_group
    .filter(F.col("opened_on_year") <= 2025)
    .select("opened_on_year", "distinct_ids", "n_upper_trim_wh_code_like_b")
    .orderBy(F.col("n_upper_trim_wh_code_like_b").asc())
)
