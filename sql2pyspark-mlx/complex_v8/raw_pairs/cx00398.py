from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_opened_on_month = Window.partitionBy("city", F.date_trunc("month", "opened_on"))
w_opened_on_month_city_by_capa = Window.partitionBy(F.month("opened_on"), "city").orderBy("capacity", "id")
w_all_by_move_count_running = Window.orderBy("move_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_count = Window.partitionBy("move_count")
w_all_by_move_count_ties = Window.orderBy("move_count")

recent_warehouses = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

with_trends = (
    recent_warehouses
    .select(
        "*",
        F.count(F.lit(1)).over(w_city_opened_on_month).alias("rows_in_city_opened_on_month"),
        F.lead("capacity").over(w_opened_on_month_city_by_capa).alias("next_capacity"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator") != "op_eve", 1)).alias("move_operator_count"),
    )
)

with_stats = (
    with_trends.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("move_move_type_count"),
        F.count("*").alias("move_count"),
        F.max("qty").alias("max_move_qty"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.max_move_qty",
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.min("move_operator_count").over(w_all_by_move_count_running).alias("min_move_operator_count_so_far"),
        F.max("max_move_qty").over(w_move_count).alias("move_count_max_max_move_qty"),
        F.count(F.lit(1)).over(w_move_count).alias("rows_in_move_count"),
        F.sum("move_move_type_count").over(w_move_count).alias("move_count_sum_move_move_type_count"),
        F.dense_rank().over(w_all_by_move_count_ties).alias("move_count_dense_rank"),
    )
)

result = (
    sequenced
    .select("id", "move_count", "next_capacity")
    .unpivot(
        ids=["id"],
        values=["move_count", "next_capacity"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
