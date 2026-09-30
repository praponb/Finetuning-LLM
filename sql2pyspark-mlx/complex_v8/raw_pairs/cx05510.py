from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id")
w_move_type_by_least_unit_pric = Window.partitionBy("move_type").orderBy("least_unit_price_minus_qty_moved_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_least_unit_pric_2 = Window.partitionBy("move_type").orderBy("least_unit_price_minus_qty_moved_at_month", "id")
w_warehouse_id_move_type = Window.partitionBy("warehouse_id", "move_type")
w_move_type_by_unit_price_runn = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_least_unit_pric_3 = Window.partitionBy("move_type").orderBy("least_unit_price_minus_qty_moved_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_moves = (
    df_stock_moves
    .filter(F.col("qty") < 370)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
    )
)

qualified = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
    .filter(F.col("operator") != "op_bee")
)

labeled = (
    qualified
    .select(
        "*",
        F.least(F.col("unit_price_minus_qty"), F.col("moved_at_month")).alias("least_unit_price_minus_qty_moved_at_month"),
        F.when(F.col("unit_price") >= 328.3, "A").when(F.col("unit_price") >= 200.7, "B").otherwise("C").alias("unit_price_tier"),
        (F.col("unit_price") + F.col("unit_price_minus_qty")).alias("unit_price_plus_unit_price_minus_qty"),
    )
)

with_trends = (
    labeled
    .select(
        "*",
        F.row_number().over(w_move_type_by_unit_price).alias("rn"),
        F.first("qty", ignorenulls=True).over(w_move_type_by_least_unit_pric).alias("first_known_qty"),
        F.ntile(3).over(w_move_type_by_least_unit_pric_2).alias("least_unit_price_minus_qty_moved_at_month_tercil"),
        F.count(F.lit(1)).over(w_warehouse_id_move_type).alias("rows_in_warehouse_id_move_type"),
        F.min("qty").over(w_move_type_by_unit_price_runn).alias("min_qty_so_far"),
        F.nth_value("moved_at_month", 2).over(w_move_type_by_least_unit_pric_3).alias("second_moved_at_month"),
    )
)

result = (
    with_trends
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "moved_at_month",
        "unit_price_minus_qty",
        "first_known_qty",
        "least_unit_price_minus_qty_moved_at_month_tercil",
        "min_qty_so_far",
        "second_moved_at_month",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
    .orderBy("first_known_qty")
)
