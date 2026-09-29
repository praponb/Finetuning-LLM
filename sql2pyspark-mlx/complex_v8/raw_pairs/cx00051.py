from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_move_type_len_ti = Window.partitionBy("operator").orderBy("move_type_len")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_operator_by_move_type_len = Window.partitionBy("operator").orderBy("move_type_len", "id")
w_warehouse_id_by_move_type_le = Window.partitionBy("warehouse_id").orderBy(F.col("move_type_len").desc(), "id")
w_sku_id_by_sku_unit_cost_ties = Window.partitionBy("sku_id").orderBy(F.col("sku_unit_cost").desc_nulls_first())
w_sku_id_by_sku_unit_cost = Window.partitionBy("sku_id").orderBy(F.col("sku_unit_cost").desc_nulls_first(), "id")
w_warehouse_id_by_move_type_le_2 = Window.partitionBy("warehouse_id").orderBy(F.col("move_type_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_move_type_len_mo = Window.partitionBy("operator").orderBy("move_type_len", "id").rowsBetween(-1, Window.currentRow)

cleaned = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

with_parent_sku = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.move_type_len",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

ranked = (
    with_parent_sku
    .select(
        "*",
        F.dense_rank().over(w_operator_by_move_type_len_ti).alias("move_type_len_dense_rank"),
        F.round(F.avg("qty").over(w_warehouse_id_sku_id), 2).alias("warehouse_id_sku_id_avg_qty"),
        F.sum("qty").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_sum_qty"),
        F.lead("sku_weight_kg").over(w_operator_by_move_type_len).alias("next_sku_weight_kg"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_id_by_move_type_le))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.rank().over(w_sku_id_by_sku_unit_cost_ties).alias("sku_unit_cost_rank"),
        F.lead("sku_unit_cost", 3).over(w_warehouse_id_by_move_type_le).alias("next3_sku_unit_cost"),
        F.row_number().over(w_sku_id_by_sku_unit_cost).alias("rn"),
        F.first("moved_at").over(w_warehouse_id_by_move_type_le_2).alias("first_moved_at"),
        F.round(F.avg("sku_unit_cost").over(w_operator_by_move_type_len_mo), 2).alias("sku_unit_cost_avg_2"),
        F.lead("move_type_len", 2).over(w_sku_id_by_sku_unit_cost).alias("next2_move_type_len"),
        F.round(F.col("sku_weight_kg") - F.avg("sku_weight_kg").over(w_warehouse_id_sku_id), 2).alias("sku_weight_kg_vs_warehouse_id_sku_id_avg"),
        F.size(F.collect_set("operator").over(w_warehouse_id_sku_id)).alias("distinct_operator_in_warehouse_id_sku_id"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_warehouse_id_sku_id), 2).alias("sku_unit_cost_vs_warehouse_id_sku_id_avg"),
    )
)

top_rows = (
    ranked
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    top_rows
    .filter(F.col("next2_move_type_len") <= 13)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "move_type_len",
        "sku_unit_cost",
        "sku_weight_kg",
        "move_type_len_dense_rank",
        "warehouse_id_sku_id_avg_qty",
        "warehouse_id_sku_id_sum_qty",
        "next_sku_weight_kg",
        "rows_in_warehouse_id_sku_id",
        "minutes_since_prev_moved_at",
        "sku_unit_cost_rank",
        "next3_sku_unit_cost",
        "first_moved_at",
        "sku_unit_cost_avg_2",
        "next2_move_type_len",
        "distinct_operator_in_warehouse_id_sku_id",
    )
)
