from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_moved_at_to_date_wareho = Window.partitionBy(F.to_date("move_moved_at"), "warehouse_id").orderBy("qty", "id", "move_id")
w_move_operator = Window.partitionBy("move_operator")
w_move_moved_at_to_date_wareho_2 = Window.partitionBy(F.to_date("move_moved_at"), "warehouse_id").orderBy("qty", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_move_unit_price = Window.orderBy(F.col("move_unit_price").desc(), "id", "move_id")
w_first_known_moved_at_date_mo = Window.partitionBy(F.month("first_known_moved_at_date")).orderBy("move_unit_price", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_decile_move_id = Window.partitionBy("qty_decile", "move_id")

valid_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-08 12:00:00").cast("timestamp")) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

enriched = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at_date",
        "operator_ref",
        F.substring(F.trim(F.col("operator_ref")), 1, 2).alias("operator_ref_prefix"),
    )
)

with_children = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at_date",
        "s.operator_ref_prefix",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
        "st.move_type",
    )
)

ranked = (
    with_children
    .select(
        "*",
        F.ntile(10).over(w_move_moved_at_to_date_wareho).alias("qty_decile"),
        F.count(F.lit(1)).over(w_move_operator).alias("rows_in_move_operator"),
        F.first("moved_at_date", ignorenulls=True).over(w_move_moved_at_to_date_wareho_2).alias("first_known_moved_at_date"),
        F.sum("move_unit_price").over(w_move_moved_at_to_date_wareho_2).alias("running_move_unit_price"),
        F.round(F.col("move_unit_price") / F.nullif(F.sum("move_unit_price").over(w_move_operator), F.lit(0)), 4).alias("move_unit_price_share_of_move_operator"),
    )
)

tagged = (
    ranked
    .filter(F.col("qty_decile") > 5)
    .select(
        "*",
        F.dayofweek(F.col("move_moved_at")).alias("move_moved_at_dayofweek"),
        F.when(F.col("rows_in_move_operator") >= 13, "large").when(F.col("rows_in_move_operator") >= 7, "medium").otherwise("small").alias("rows_in_move_operator_tier"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("rows_in_move_operator").cast("string")).alias("move_type_ref"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.ntile(10).over(w_all_by_move_unit_price).alias("move_unit_price_decile"),
        F.sum("move_moved_at_dayofweek").over(w_first_known_moved_at_date_mo).alias("running_move_moved_at_dayofweek"),
        F.size(F.collect_set("move_operator").over(w_qty_decile_move_id)).alias("distinct_move_operator_in_qty_decile_move_id"),
        F.sum("qty_decile").over(w_qty_decile_move_id).alias("qty_decile_move_id_sum_qty_decile"),
    )
)

result = (
    windowed
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at_date",
        "operator_ref_prefix",
        "move_id",
        "move_moved_at",
        "move_type",
        "qty_decile",
        "first_known_moved_at_date",
        "move_unit_price_share_of_move_operator",
        "move_moved_at_dayofweek",
        "rows_in_move_operator_tier",
        "move_type_ref",
        "move_unit_price_decile",
        "running_move_moved_at_dayofweek",
        "distinct_move_operator_in_qty_decile_move_id",
        "qty_decile_move_id_sum_qty_decile",
        F.last_day(F.col("moved_at_date")).alias("moved_at_date_month_end"),
    )
)
