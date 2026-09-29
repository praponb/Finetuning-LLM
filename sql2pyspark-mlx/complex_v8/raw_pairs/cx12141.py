from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_move_type_operator_by_qty_pe = Window.partitionBy("move_type", "operator").orderBy("qty_per_unit_price")
w_move_type_operator_by_qty_pe_2 = Window.partitionBy("move_type", "operator").orderBy("qty_per_unit_price", "id")
w_move_type_operator_by_qty_pe_3 = Window.partitionBy("move_type", "operator").orderBy("qty_per_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (~F.col("operator").isin("op_chan", "op_eve")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price_2"),
    )
)

metrics = (
    base
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty_per_unit_price",
        "qty_per_unit_price_2",
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.rank().over(w_move_type_operator_by_qty_pe).alias("qty_per_unit_price_rank"),
        F.lead("qty_per_unit_price_2", 1, 0).over(w_move_type_operator_by_qty_pe_2).alias("next_qty_per_unit_price_2"),
        F.first("moved_at").over(w_move_type_operator_by_qty_pe_3).alias("first_moved_at"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.coalesce(F.col("qty_per_unit_price_2"), F.lit(0.0)).alias("qty_per_unit_price_2_filled"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

result = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "rows_in_warehouse_id",
        "qty_per_unit_price_rank",
        "next_qty_per_unit_price_2",
        "first_moved_at",
        "qty_per_unit_price_2_filled",
        "move_type_len",
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
    .orderBy(F.col("qty_per_unit_price_rank").asc_nulls_last())
)
