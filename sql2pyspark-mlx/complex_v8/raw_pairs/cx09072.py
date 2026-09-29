from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_moved_at_year_b = Window.partitionBy("warehouse_id", F.year("moved_at")).orderBy("sku_weight_kg", "id")
w_warehouse_id_by_sku_weight_k = Window.partitionBy("warehouse_id").orderBy(F.col("sku_weight_kg").desc(), "id")
w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_move_type_by_sku_weight_kg = Window.partitionBy("move_type").orderBy(F.col("sku_weight_kg").desc(), "id")
w_move_type_operator_by_qty_mo = Window.partitionBy("move_type", "operator").orderBy(F.col("qty").desc(), "id").rowsBetween(-1, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_weight_kg_tercile_by_uni = Window.partitionBy("sku_weight_kg_tercile").orderBy("unit_price")
w_operator_by_sku_weight_kg_cu = Window.partitionBy("operator").orderBy(F.col("sku_weight_kg_cume_dist").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_sku_weight_kg_tercile_by_uni_2 = Window.partitionBy("sku_weight_kg_tercile").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = df_stock_moves.filter(F.col("warehouse_id") >= 2)

enriched = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

metrics = (
    enriched
    .select(
        "*",
        (F.col("sku_weight_kg") - F.lag("sku_weight_kg").over(w_warehouse_id_moved_at_year_b)).alias("sku_weight_kg_change"),
        F.dense_rank().over(w_warehouse_id_by_sku_weight_k).alias("sku_weight_kg_dense_rank"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_move_type_sku_id), F.lit(0)), 4).alias("qty_share_of_move_type_sku_id"),
        F.ntile(3).over(w_warehouse_id_by_sku_weight_k).alias("sku_weight_kg_tercile"),
        F.round(F.avg("unit_price").over(w_move_type_sku_id), 2).alias("move_type_sku_id_avg_unit_price"),
        F.round(F.cume_dist().over(w_move_type_by_sku_weight_kg), 3).alias("sku_weight_kg_cume_dist"),
        F.max("warehouse_capacity").over(w_move_type_operator_by_qty_mo).alias("warehouse_capacity_max_2"),
        F.sum("sku_weight_kg").over(w_warehouse_id).alias("warehouse_id_sum_sku_weight_kg"),
        F.lead("qty").over(w_move_type_by_sku_weight_kg).alias("next_qty"),
    )
)

top_per_group = metrics.filter(F.col("sku_weight_kg_dense_rank") == 1)

sequenced = (
    top_per_group
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "sku_weight_kg",
        "sku_code",
        "sku_weight_kg_dense_rank",
        "qty_share_of_move_type_sku_id",
        "sku_weight_kg_tercile",
        "sku_weight_kg_cume_dist",
        "warehouse_capacity_max_2",
        "warehouse_id_sum_sku_weight_kg",
        "next_qty",
        F.round(F.cume_dist().over(w_sku_weight_kg_tercile_by_uni), 3).alias("unit_price_cume_dist"),
        F.round(F.avg("sku_weight_kg_dense_rank").over(w_operator_by_sku_weight_kg_cu), 2).alias("sku_weight_kg_dense_rank_avg_2_prior"),
        F.count(F.lit(1)).over(w_warehouse_id_operator).alias("rows_in_warehouse_id_operator"),
        F.sum("sku_weight_kg_change").over(w_sku_weight_kg_tercile_by_uni_2).alias("running_sku_weight_kg_change"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_operator), 2).alias("qty_vs_warehouse_id_operator_avg"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "sku_weight_kg",
        "sku_code",
        "qty_share_of_move_type_sku_id",
        "sku_weight_kg_tercile",
        "sku_weight_kg_cume_dist",
        "warehouse_capacity_max_2",
        "next_qty",
        "unit_price_cume_dist",
        "rows_in_warehouse_id_operator",
        "running_sku_weight_kg_change",
    )
    .orderBy("sku_weight_kg")
)
