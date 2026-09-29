from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_city_group = Window.partitionBy("city", "city_group")
w_all_by_capacity_filled_runni = Window.orderBy("capacity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_capacity_filled = Window.orderBy("capacity_filled", "id")
w_city_group_by_capacity = Window.partitionBy("city_group").orderBy("capacity", "id")
w_city_group_city = Window.partitionBy("city_group", "city")
w_opened_on_month_by_rows_in_c = Window.partitionBy(F.date_trunc("month", "opened_on")).orderBy("rows_in_city_city_group", "id")
w_city = Window.partitionBy("city")
w_opened_on_month_by_rows_in_c_2 = Window.partitionBy(F.date_trunc("month", "opened_on")).orderBy("rows_in_city_city_group", "id").rowsBetween(-6, Window.currentRow)
w_dedup = Window.partitionBy("city", "city_group").orderBy(F.col("capacity_dense_rank").desc(), F.col("id").desc())

filtered_warehouses = (
    df_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.when(F.col("city").isin("Bangkok", "Singapore"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
    )
)

enriched = (
    filtered_warehouses
    .filter(F.year(F.col("opened_on")) == 2024)
    .select("*", F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"))
)

sequenced = (
    enriched
    .select(
        "*",
        F.sum("capacity_filled").over(w_city_city_group).alias("city_city_group_sum_capacity_filled"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_city_city_group), F.lit(0)), 4).alias("capacity_share_of_city_city_group"),
        F.count(F.lit(1)).over(w_city_city_group).alias("rows_in_city_city_group"),
        F.sum("capacity").over(w_all_by_capacity_filled_runni).alias("running_capacity"),
        F.lead("capacity_filled").over(w_all_by_capacity_filled).alias("next_capacity_filled"),
    )
)

metrics = (
    sequenced
    .withColumn("capacity_dense_rank", F.dense_rank().over(w_city_group_by_capacity))
    .withColumn("capacity_share_of_city_group_city", F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_city_group_city), F.lit(0)), 4))
)

windowed = (
    metrics
    .select(
        "*",
        F.lag("capacity_dense_rank").over(w_opened_on_month_by_rows_in_c).alias("prev_capacity_dense_rank"),
        F.round(F.cume_dist().over(w_opened_on_month_by_rows_in_c), 3).alias("rows_in_city_city_group_cume_dist"),
        F.size(F.collect_set("city_group").over(w_city)).alias("distinct_city_group_in_city"),
        F.round(F.avg("capacity_filled").over(w_opened_on_month_by_rows_in_c_2), 2).alias("capacity_filled_avg_7"),
    )
)

latest_per_key = (
    windowed
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "capacity_filled",
        "city_city_group_sum_capacity_filled",
        "capacity_share_of_city_city_group",
        "rows_in_city_city_group",
        "next_capacity_filled",
        "capacity_dense_rank",
        "capacity_share_of_city_group_city",
        "rows_in_city_city_group_cume_dist",
        "distinct_city_group_in_city",
    )
    .orderBy("id")
)
