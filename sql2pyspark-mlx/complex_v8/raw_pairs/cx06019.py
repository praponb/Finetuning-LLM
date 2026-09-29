from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_moved_at_hour_by_ware = Window.partitionBy("sku_id", "moved_at_hour").orderBy(F.col("warehouse_opened_on").desc())
w_all_by_qty_full = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")

valid_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.hour(F.col("moved_at")).alias("moved_at_hour"),
    )
)

qualified = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

with_details = (
    qualified.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.operator_or_unknown",
        "s.moved_at_hour",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

with_trends = (
    with_details
    .select(
        "*",
        F.round(F.cume_dist().over(w_sku_id_moved_at_hour_by_ware), 3).alias("warehouse_opened_on_cume_dist"),
        F.nth_value("warehouse_opened_on", 2).over(w_all_by_qty_full).alias("second_warehouse_opened_on"),
        F.round(F.percent_rank().over(w_all_by_qty), 3).alias("qty_percent_rank"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.size(F.col("sk.labels")) >= 2), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "qty_percent_rank",
        "warehouse_opened_on_cume_dist",
        "unit_price",
    )
    .unpivot(
        ids=["id"],
        values=["qty_percent_rank", "warehouse_opened_on_cume_dist", "unit_price"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
