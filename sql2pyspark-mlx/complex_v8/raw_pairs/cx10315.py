from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type_by_sk = Window.partitionBy("warehouse_id", "move_type").orderBy("sku_weight_kg", "id")
w_move_type_by_qty_ties = Window.partitionBy("move_type").orderBy(F.col("qty").desc_nulls_first())
w_all_by_sku_weight_kg_full = Window.orderBy(F.col("sku_weight_kg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").desc_nulls_first(), "id")
w_move_type = Window.partitionBy("move_type")
w_warehouse_id_move_type_by_sk_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_qty_full = Window.partitionBy("move_type").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_sku_weight_kg_rank_next_move = Window.partitionBy("sku_weight_kg_rank", F.year("next_moved_at")).orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_qty_vs_move_type_a = Window.partitionBy("sku_id").orderBy("qty_vs_move_type_avg", "id").rowsBetween(-1, Window.currentRow)
w_next_moved_at_month_qty_dens = Window.partitionBy(F.date_trunc("month", "next_moved_at"), "qty_dense_rank")
w_sku_id_by_qty_vs_move_type_a_2 = Window.partitionBy("sku_id").orderBy("qty_vs_move_type_avg", "id")
w_sku_id_by_qty_vs_move_type_a_3 = Window.partitionBy("sku_id").orderBy("qty_vs_move_type_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_weight_kg_rank_next_move_2 = Window.partitionBy("sku_weight_kg_rank", F.year("next_moved_at")).orderBy("moved_at", "id")
w_sku_weight_kg_rank_next_move_3 = Window.partitionBy("sku_weight_kg_rank", F.year("next_moved_at")).orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stock_moves
    .filter((F.col("move_type").isin("INBOUND", "OUTBOUND")) & (F.col("operator") == "op_bee"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

joined = (
    base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

windowed = (
    joined
    .select(
        "*",
        F.rank().over(w_warehouse_id_move_type_by_sk).alias("sku_weight_kg_rank"),
        F.dense_rank().over(w_move_type_by_qty_ties).alias("qty_dense_rank"),
        F.first("qty").over(w_all_by_sku_weight_kg_full).alias("first_qty"),
        F.lag("qty", 2).over(w_move_type_by_qty).alias("prev2_qty"),
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_move_type), 2).alias("sku_unit_cost_vs_move_type_avg"),
        F.sum("sku_weight_kg").over(w_warehouse_id_move_type_by_sk_2).alias("running_sku_weight_kg"),
        F.size(F.collect_set("warehouse_id").over(w_move_type)).alias("distinct_warehouse_id_in_move_type"),
        F.round(F.col("qty") - F.avg("qty").over(w_move_type), 2).alias("qty_vs_move_type_avg"),
        F.min("sku_unit_cost").over(w_move_type).alias("move_type_min_sku_unit_cost"),
        F.lead("moved_at").over(w_move_type_by_qty).alias("next_moved_at"),
        F.nth_value("unit_price", 2).over(w_move_type_by_qty_full).alias("second_unit_price"),
        F.ntile(3).over(w_all_by_unit_price).alias("unit_price_tercile"),
        F.round(F.avg("sku_weight_kg").over(w_move_type), 2).alias("move_type_avg_sku_weight_kg"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "sku_weight_kg",
        "qty_dense_rank",
        "first_qty",
        "running_sku_weight_kg",
        "qty_vs_move_type_avg",
        "move_type_min_sku_unit_cost",
        "next_moved_at",
        "second_unit_price",
        "unit_price_tercile",
        F.first("move_type_avg_sku_weight_kg").over(w_sku_weight_kg_rank_next_move).alias("first_move_type_avg_sku_weight_kg"),
        F.round(F.avg("qty_dense_rank").over(w_sku_id_by_qty_vs_move_type_a), 2).alias("qty_dense_rank_avg_2"),
        F.count(F.lit(1)).over(w_next_moved_at_month_qty_dens).alias("rows_in_next_moved_at_month_qty_dense_rank"),
        F.row_number().over(w_sku_id_by_qty_vs_move_type_a_2).alias("qty_vs_move_type_avg_seq"),
        F.last("moved_at").over(w_sku_id_by_qty_vs_move_type_a_3).alias("last_moved_at"),
        (F.col("move_type_min_sku_unit_cost") - F.lag("move_type_min_sku_unit_cost").over(w_sku_weight_kg_rank_next_move_2)).alias("move_type_min_sku_unit_cost_change"),
        F.sum("qty_dense_rank").over(w_sku_weight_kg_rank_next_move_3).alias("running_qty_dense_rank"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        "sku_weight_kg",
        "qty_dense_rank",
        "running_sku_weight_kg",
        "qty_vs_move_type_avg",
        "move_type_min_sku_unit_cost",
        "second_unit_price",
        "unit_price_tercile",
        "qty_dense_rank_avg_2",
        "rows_in_next_moved_at_month_qty_dense_rank",
        "last_moved_at",
        "running_qty_dense_rank",
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)
