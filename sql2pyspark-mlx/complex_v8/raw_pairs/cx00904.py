from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_week_ties = Window.orderBy(F.col("moved_at_week").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_moved_at_week = Window.orderBy(F.col("moved_at_week").desc(), "id")
w_all_by_moved_at_week_running = Window.orderBy(F.col("moved_at_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")

scoped = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_chan", "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.round(F.percent_rank().over(w_all_by_moved_at_week_ties), 3).alias("moved_at_week_percent_rank"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.lead("unit_price").over(w_all_by_moved_at_week).alias("next_unit_price"),
        F.min("unit_price").over(w_all_by_moved_at_week_running).alias("min_unit_price_so_far"),
    )
)

with_parent = (
    with_trends.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.min_unit_price_so_far",
        F.col("sk.labels").alias("sku_labels"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "min_unit_price_so_far",
        "sku_labels",
        F.min("unit_price").over(w_sku_id_move_type).alias("sku_id_move_type_min_unit_price"),
        (F.col("min_unit_price_so_far") - F.lag("min_unit_price_so_far").over(w_warehouse_id_by_unit_price)).alias("min_unit_price_so_far_change"),
    )
)

result = (
    windowed
    .select("id", "min_unit_price_so_far", "min_unit_price_so_far_change")
    .unpivot(
        ids=["id"],
        values=["min_unit_price_so_far", "min_unit_price_so_far_change"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
