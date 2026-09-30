from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_sku_unit_cost_r = Window.partitionBy("move_type").orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_sku_unit_cost_t = Window.partitionBy("move_type").orderBy("sku_unit_cost")
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_warehouse_id_by_sku_unit_cos = Window.partitionBy("warehouse_id").orderBy(F.col("sku_unit_cost").desc())
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_by_sku_unit_cos_2 = Window.partitionBy("warehouse_id").orderBy(F.col("sku_unit_cost").desc(), "id")

recent_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_details_sku = (
    recent_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.length(F.trim(F.col("sk.sku_code"))) >= 7)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        "sk.sku_code",
    )
)

windowed = (
    with_details_sku
    .select(
        "*",
        F.last("unit_price", ignorenulls=True).over(w_move_type_by_sku_unit_cost_r).alias("last_known_unit_price"),
        F.dense_rank().over(w_move_type_by_sku_unit_cost_t).alias("sku_unit_cost_dense_rank"),
        F.size(F.collect_set("operator").over(w_sku_id_move_type)).alias("distinct_operator_in_sku_id_move_type"),
        F.rank().over(w_warehouse_id_by_sku_unit_cos).alias("sku_unit_cost_rank"),
        F.max("sku_unit_cost").over(w_warehouse_id).alias("warehouse_id_max_sku_unit_cost"),
        F.ntile(4).over(w_warehouse_id_by_sku_unit_cos_2).alias("sku_unit_cost_quartile"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
    )
)

leaders = windowed.filter(F.col("sku_unit_cost_rank") == 1)

result = (
    leaders
    .filter(F.col("sku_unit_cost_quartile") <= 9)
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "sku_unit_cost",
        "sku_code",
        "sku_unit_cost_dense_rank",
        "distinct_operator_in_sku_id_move_type",
        "warehouse_id_max_sku_unit_cost",
        "sku_unit_cost_quartile",
        "rows_in_sku_id_move_type",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
    .orderBy(F.col("sku_code").asc_nulls_last(), "id")
    .limit(20)
)
