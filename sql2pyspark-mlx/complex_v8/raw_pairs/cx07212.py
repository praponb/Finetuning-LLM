from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_sku_unit_cos = Window.partitionBy("warehouse_id").orderBy("sku_unit_cost", "id")
w_sku_id_by_qty_ties = Window.partitionBy("sku_id").orderBy(F.col("qty").desc())
w_move_type = Window.partitionBy("move_type")
w_all_by_moved_at = Window.orderBy("moved_at", "id")

cleaned = (
    df_stock_moves
    .filter((~F.col("operator").isin("op_eve", "op_chan")) & (F.col("unit_price") >= 111.7))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.abs(F.col("unit_price") - 264.8).alias("unit_price_distance"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.upper(F.trim(F.col("w.wh_code"))).like("B%")), "left_semi")
)

joined = (
    qualified.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.category") != "Toys")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price_distance",
        "s.moved_at_label",
        "s.operator_clean",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        (F.col("unit_price_distance") - F.lag("unit_price_distance").over(w_warehouse_id_by_sku_unit_cos)).alias("unit_price_distance_change"),
        F.dense_rank().over(w_sku_id_by_qty_ties).alias("qty_dense_rank"),
        F.round(F.avg("unit_price_distance").over(w_move_type), 2).alias("move_type_avg_unit_price_distance"),
        F.lag("moved_at", 2).over(w_all_by_moved_at).alias("prev2_moved_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        F.col("move_type_avg_unit_price_distance").alias("move_type_avg_unit_price_distance_v"),
        F.col("unit_price_distance_change").alias("m_unit_price_distance_change"),
        F.col("sku_unit_cost").alias("sku_unit_cost_v"),
    )
    .unpivot(
        ids=["id"],
        values=["move_type_avg_unit_price_distance_v", "m_unit_price_distance_change", "sku_unit_cost_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
