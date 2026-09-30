from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_month_city_by_open = Window.partitionBy(F.month("opened_on"), "city").orderBy(F.col("opened_on").desc(), "id")
w_opened_on_month_city_by_open_2 = Window.partitionBy(F.month("opened_on"), "city").orderBy(F.col("opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_on_month_city_by_open_3 = Window.partitionBy(F.month("opened_on"), "city").orderBy(F.col("opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_opened_on = Window.orderBy("first_known_opened_on", "id")
w_all_by_first_known_opened_on_2 = Window.orderBy("first_known_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_opened_on_month_opened = Window.partitionBy(F.month("next_opened_on"), "opened_on_dense_rank")
w_latest = Window.partitionBy("city").orderBy(F.col("second_next_opened_on").desc(), F.col("id").desc())

scoped_warehouses = (
    df_warehouses
    .filter(F.col("capacity") >= 4912)
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
    )
)

sequenced = (
    scoped_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.dense_rank().over(w_opened_on_month_city_by_open).alias("opened_on_dense_rank"),
        F.first("opened_on", ignorenulls=True).over(w_opened_on_month_city_by_open_2).alias("first_known_opened_on"),
        F.nth_value("opened_on", 2).over(w_opened_on_month_city_by_open_3).alias("second_opened_on"),
        F.lead("opened_on").over(w_opened_on_month_city_by_open).alias("next_opened_on"),
    )
)

annotated = (
    sequenced
    .filter(F.col("opened_on_dense_rank") <= 6)
    .select(
        "*",
        F.date_format(F.col("next_opened_on"), "MMM yyyy").alias("next_opened_on_label"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "city",
        "opened_on",
        "opened_on_dense_rank",
        "second_opened_on",
        "next_opened_on",
        "next_opened_on_label",
        F.row_number().over(w_all_by_first_known_opened_on).alias("rn"),
        F.nth_value("next_opened_on", 2).over(w_all_by_first_known_opened_on_2).alias("second_next_opened_on"),
        F.size(F.collect_set("city").over(w_next_opened_on_month_opened)).alias("distinct_city_in_next_opened_on_month_opened_on_"),
        F.round(F.avg("opened_on_dense_rank").over(w_next_opened_on_month_opened), 2).alias("next_opened_on_month_opened_on_dense_rank_avg_op"),
        F.nth_value("opened_on_dense_rank", 2).over(w_all_by_first_known_opened_on_2).alias("second_opened_on_dense_rank"),
        F.rank().over(w_all_by_first_known_opened_on).alias("first_known_opened_on_rank"),
    )
)

deduped = (
    ranked
    .withColumn("rn_2", F.row_number().over(w_latest))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

per_group = (
    deduped
    .groupBy(
        "city",
        "rn",
        F.date_trunc("month", "second_opened_on").alias("second_opened_on_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("first_known_opened_on_rank"), 2).alias("avg_first_known_opened_on_rank"),
        F.round(F.sum("second_opened_on_dense_rank") / F.nullif(F.sum("first_known_opened_on_rank"), F.lit(0)), 3).alias("second_opened_on_dense_rank_to_first_known_opene"),
        F.sum("second_opened_on_dense_rank").alias("total_second_opened_on_dense_rank"),
    )
)

result = (
    per_group
    .filter(F.col("n_rows") >= 4)
    .select(
        "city",
        "rn",
        "second_opened_on_month",
        "second_opened_on_dense_rank_to_first_known_opene",
        "total_second_opened_on_dense_rank",
        F.coalesce(F.col("rn"), F.lit(0)).alias("rn_filled"),
    )
    .orderBy(F.col("rn").desc())
)
