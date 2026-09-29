from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator_by_qty_fu = Window.partitionBy("move_type", "operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_move_type_operator_by_qty = Window.partitionBy("move_type", "operator").orderBy(F.col("qty").desc(), "id")

scoped = (
    df_stock_moves
    .filter((F.col("unit_price").between(164.1, 330.0)) & (F.col("operator").isin("op_dew", "op_ann")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price_filled",
        F.last("moved_at").over(w_move_type_operator_by_qty_fu).alias("last_moved_at"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_sku_id), 2).alias("qty_vs_warehouse_id_sku_id_avg"),
        F.round(F.cume_dist().over(w_move_type_operator_by_qty), 3).alias("qty_cume_dist"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id_sku_id), F.lit(0)), 4).alias("qty_share_of_warehouse_id_sku_id"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("qty").alias("total_move_qty"),
        F.sum("qty").alias("total_move_qty_2"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

profiled = (
    metrics.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.total_move_qty",
        "st.total_move_qty_2",
        "st.first_moved_at",
    )
)

screened = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "TRANSFER") & (F.col("st.moved_at") >= F.lit("2025-02-12 10:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("qty_share_of_warehouse_id_sku_id").between(0.1, 0.7))
)

result = (
    screened
    .groupingSets(
        [["operator", "move_type"], ["operator"], ["move_type"], []],
        "operator", "move_type",
    )
    .agg(
        F.grouping("operator").alias("is_total_operator"),
        F.min("first_moved_at").alias("first_first_moved_at"),
    )
    .select(
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "is_total_operator",
        "first_first_moved_at",
    )
    .orderBy(F.col("operator").asc_nulls_last(), F.col("move_type").asc_nulls_last())
)
