from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_capacity_filled_prio = Window.partitionBy("city").orderBy(F.col("capacity_filled").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_all_by_capacity_filled = Window.orderBy("capacity_filled", "id")
w_n_city_is_not_null_distinct = Window.partitionBy("n_city_is_not_null", "distinct_ids").orderBy(F.col("distinct_ids").desc(), "opened_on_ym").rowsBetween(-1, -1)
w_n_city_is_not_null_distinct_2 = Window.partitionBy("n_city_is_not_null", "distinct_ids").orderBy(F.col("distinct_ids").desc(), "opened_on_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_distinct_ids_by_ro = Window.partitionBy("row_count", "distinct_ids").orderBy(F.col("row_count").desc())
w_row_count_distinct_ids_by_ro_2 = Window.partitionBy("row_count", "distinct_ids").orderBy(F.col("row_count").desc(), "opened_on_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_city_is_not_null_distinct_3 = Window.partitionBy("n_city_is_not_null", "distinct_ids").orderBy(F.col("distinct_ids").desc(), "opened_on_ym")

filtered = (
    df_warehouses
    .filter(F.col("capacity") <= 11506)
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.date_format(F.col("opened_on"), "MMM yyyy").alias("opened_on_label"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.round(F.avg("capacity").over(w_city_by_capacity_filled_prio), 2).alias("capacity_avg_1_prior"),
        F.lag("opened_on").over(w_all_by_capacity_filled).alias("prev_opened_on"),
    )
)

grouped = (
    sequenced
    .groupBy(F.date_format("opened_on", "yyyy-MM").alias("opened_on_ym"))
    .agg(
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city").isNotNull(), 1)).alias("n_city_is_not_null"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
    )
)

with_trends = (
    grouped
    .select(
        "opened_on_ym",
        "n_city_is_not_null",
        "distinct_ids",
        "row_count",
        F.round(F.avg("distinct_ids").over(w_n_city_is_not_null_distinct), 2).alias("distinct_ids_avg_1_prior"),
        F.nth_value("distinct_ids", 2).over(w_n_city_is_not_null_distinct_2).alias("second_distinct_ids"),
        F.rank().over(w_row_count_distinct_ids_by_ro).alias("row_count_rank"),
        F.last("n_city_is_not_null").over(w_row_count_distinct_ids_by_ro_2).alias("last_n_city_is_not_null"),
        F.lag("distinct_ids").over(w_n_city_is_not_null_distinct_3).alias("prev_distinct_ids"),
    )
)

result = (
    with_trends
    .select(
        "opened_on_ym",
        "distinct_ids",
        "row_count",
        "distinct_ids_avg_1_prior",
        "row_count_rank",
        "last_n_city_is_not_null",
        "prev_distinct_ids",
        (F.floor(F.col("n_city_is_not_null") / 2) * 2).alias("n_city_is_not_null_bucket"),
    )
    .orderBy(F.col("distinct_ids_avg_1_prior").asc_nulls_last())
)
