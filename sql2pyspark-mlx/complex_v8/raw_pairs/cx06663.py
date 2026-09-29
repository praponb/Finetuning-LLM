from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_id = Window.partitionBy("move_id")
w_move_type_move_id_by_move_un = Window.partitionBy("move_type", "move_id").orderBy(F.col("move_unit_price").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_id_move_operator_by_mov = Window.partitionBy("move_id", "move_operator").orderBy("move_unit_price", "id", "move_id")

recent_warehouses = (
    df_warehouses
    .filter((F.col("city") == "Singapore") & (F.col("opened_on").between(F.lit("2024-02-19").cast("date"), F.lit("2025-07-03").cast("date"))))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
    )
)

line_level_moves = (
    recent_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.opened_on",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
        F.col("st.operator").alias("move_operator"),
    )
)

with_trends = (
    line_level_moves
    .select(
        "*",
        F.round(F.col("move_unit_price") - F.avg("move_unit_price").over(w_move_id), 2).alias("move_unit_price_vs_move_id_avg"),
        F.max("move_unit_price").over(w_move_id).alias("move_id_max_move_unit_price"),
        F.sum("move_unit_price").over(w_move_type_move_id_by_move_un).alias("running_move_unit_price"),
        F.rank().over(w_move_id_move_operator_by_mov).alias("move_unit_price_rank"),
    )
)

result = (
    with_trends
    .rollup("move_id", "move_type")
    .agg(
        F.grouping("move_id").alias("is_total_move_id"),
        F.sum(F.when(F.col("move_operator") == "op_bee", F.col("move_unit_price")).otherwise(0)).alias("move_unit_price_move_operator_op_bee"),
        F.countDistinct("id").alias("distinct_ids"),
        F.datediff(F.max("opened_on"), F.min("opened_on")).alias("opened_on_span_days"),
    )
    .orderBy(F.col("move_id").asc_nulls_last(), F.col("move_type").asc_nulls_last())
)
