from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_year = Window.partitionBy(F.year("opened_on"))
w_move_operator_count_city_by = Window.partitionBy("move_operator_count", "city").orderBy(F.col("opened_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_operator_count_city_by_2 = Window.partitionBy("move_operator_count", "city").orderBy(F.col("opened_on").desc_nulls_first(), "id")

prepared = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("C%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.substring(F.trim(F.col("wh_code")), 1, 3).alias("wh_code_prefix"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("unit_price").alias("min_move_unit_price"),
        F.max("unit_price").alias("max_move_unit_price"),
        F.count(F.when(F.col("operator") != "op_dew", 1)).alias("move_operator_count"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.min_move_unit_price",
        "st.max_move_unit_price",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

windowed = (
    with_counts
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "wh_code_prefix",
        "min_move_unit_price",
        "max_move_unit_price",
        F.sum("capacity").over(w_opened_on_year).alias("opened_on_year_sum_capacity"),
        F.count(F.lit(1)).over(w_move_operator_count_city_by).alias("running_count"),
        F.round(F.col("move_operator_count") - F.avg("move_operator_count").over(w_opened_on_year), 2).alias("move_operator_count_vs_opened_on_year_avg"),
        F.lag("capacity").over(w_move_operator_count_city_by_2).alias("prev_capacity"),
        F.sum("move_operator_count").over(w_opened_on_year).alias("opened_on_year_sum_move_operator_count"),
    )
)

result = (
    windowed
    .filter(F.col("move_operator_count_vs_opened_on_year_avg") <= 1.6)
    .select(
        "id",
        "wh_code",
        "city",
        "wh_code_prefix",
        "min_move_unit_price",
        "max_move_unit_price",
        "opened_on_year_sum_capacity",
        "running_count",
        "prev_capacity",
        "opened_on_year_sum_move_operator_count",
        F.add_months(F.col("opened_on"), 6).alias("opened_on_plus_6m"),
        F.date_trunc("quarter", F.col("opened_on")).alias("opened_on_quarter"),
    )
    .orderBy(F.col("wh_code").desc())
)
