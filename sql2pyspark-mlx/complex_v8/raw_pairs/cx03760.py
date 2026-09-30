from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_move_type_move_type_by = Window.partitionBy("move_move_type", "move_type").orderBy("moved_at", "id", "move_id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price_distance_f = Window.orderBy(F.col("unit_price_distance").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_stock_moves
    .filter((F.col("operator").isin("op_eve", "op_ann", "op_dew")) & (F.col("warehouse_id") >= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 161.4).alias("unit_price_distance"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
    )
)

with_children_moves = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_distance",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
    )
)

ranked = (
    with_children_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_distance",
        "move_id",
        "move_unit_price",
        "move_move_type",
        "move_operator",
        "move_qty",
        F.lead("moved_at", 3).over(w_move_move_type_move_type_by).alias("next3_moved_at"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.first("move_qty").over(w_all_by_unit_price_distance_f).alias("first_move_qty"),
        F.row_number().over(w_move_move_type_move_type_by).alias("rn"),
        F.size(F.collect_set("move_move_type").over(w_warehouse_id)).alias("distinct_move_move_type_in_warehouse_id"),
    )
)

result = (
    ranked
    .groupingSets(
        [["warehouse_id", "distinct_sku_id_in_warehouse_id"], ["warehouse_id"]],
        "warehouse_id", "distinct_sku_id_in_warehouse_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("cnt"),
        F.percentile_approx("distinct_move_move_type_in_warehouse_id", 0.5).alias("median_distinct_move_move_type_in_warehouse_id"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("distinct_sku_id_in_warehouse_id").asc_nulls_last(),
    )
)
