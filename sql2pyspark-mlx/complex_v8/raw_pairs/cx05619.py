from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_month_city = Window.partitionBy(F.date_trunc("month", "opened_on"), "city")
w_opened_on_month_city_by_open = Window.partitionBy(F.date_trunc("month", "opened_on"), "city").orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_opened_on_prior_2 = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), "id").rowsBetween(-2, -1)
w_first_known_opened_on_year_c = Window.partitionBy(F.year("first_known_opened_on"), "city")
w_first_known_opened_on_month = Window.partitionBy(F.date_trunc("month", "first_known_opened_on"), "city").orderBy("opened_on_month_city_sum_capacity", "id")

cleaned_warehouses = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("capacity") <= 11005))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_format(F.col("opened_on"), "yyyy-MM").alias("opened_on_label"),
        F.concat_ws("-", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
    )
)

windowed = (
    cleaned_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "opened_on_label",
        "city_wh_code_label",
        F.sum("capacity").over(w_opened_on_month_city).alias("opened_on_month_city_sum_capacity"),
        F.sum("capacity").over(w_opened_on_month_city_by_open).alias("running_capacity"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_opened_on_month_city), 2).alias("capacity_vs_opened_on_month_city_avg"),
        F.max("capacity").over(w_city_by_opened_on_prior_2).alias("capacity_max_2_prior"),
        F.first("opened_on", ignorenulls=True).over(w_opened_on_month_city_by_open).alias("first_known_opened_on"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_opened_on_month_city), F.lit(0)), 4).alias("capacity_share_of_opened_on_month_city"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        "city_wh_code_label",
        "opened_on_month_city_sum_capacity",
        "capacity_vs_opened_on_month_city_avg",
        "capacity_max_2_prior",
        "capacity_share_of_opened_on_month_city",
        F.sum("capacity_max_2_prior").over(w_first_known_opened_on_year_c).alias("first_known_opened_on_year_city_sum_capacity_max"),
        F.lag("capacity_max_2_prior", 3, 0).over(w_first_known_opened_on_month).alias("prev3_capacity_max_2_prior"),
    )
)

with_children_moves = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.city",
        "s.opened_on_month_city_sum_capacity",
        "s.first_known_opened_on_year_city_sum_capacity_max",
        "s.prev3_capacity_max_2_prior",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    with_children_moves
    .groupingSets(
        [["move_id", "city"], ["move_id"], []],
        "move_id", "city",
    )
    .agg(
        F.grouping("move_id").alias("is_total_move_id"),
        F.sum(F.when(F.col("move_type") == "INBOUND", F.col("opened_on_month_city_sum_capacity"))).alias("opened_on_month_city_sum_capacity_move_type_inbo"),
    )
    .orderBy(F.col("move_id").asc_nulls_last(), F.col("city").asc_nulls_last())
)
