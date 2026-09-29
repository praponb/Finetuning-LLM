from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_year_by_unit_price = Window.partitionBy(F.year("moved_at")).orderBy(F.col("unit_price").desc())
w_all_by_sku_weight_kg_prior_3 = Window.orderBy("sku_weight_kg", "id").rowsBetween(-3, -1)
w_move_type_by_sku_weight_kg = Window.partitionBy("move_type").orderBy("sku_weight_kg", "id")
w_moved_at_year_by_unit_price_2 = Window.partitionBy(F.year("moved_at")).orderBy(F.col("unit_price").desc(), "id")
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_all_by_sku_weight_kg = Window.orderBy("sku_weight_kg", "id")
w_move_type = Window.partitionBy("move_type")
w_warehouse_id_sku_id_by_sku_w = Window.partitionBy("warehouse_id", "sku_id").orderBy("sku_weight_kg", "id")
w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_all_by_sku_weight_kg_running = Window.orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_year_by_unit_price_3 = Window.partitionBy(F.year("moved_at")).orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, -1)
w_warehouse_id_sku_id_by_sku_w_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy("sku_weight_kg")
w_sku_weight_kg_rank_by_distin = Window.partitionBy("sku_weight_kg_rank").orderBy("distinct_warehouse_id_in_sku_id_move_type", "id")
w_sku_weight_kg_rank_by_distin_2 = Window.partitionBy("sku_weight_kg_rank").orderBy("distinct_warehouse_id_in_sku_id_move_type")
w_sku_id = Window.partitionBy("sku_id")
w_distinct_warehouse_id_in_sku = Window.partitionBy("distinct_warehouse_id_in_sku_id_move_type").orderBy("rows_in_sku_id_move_type")

filtered = (
    df_stock_moves
    .filter((F.col("operator").isin("op_ann", "op_chan")) & (F.col("qty") > 116))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_ref",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "move_type_ref",
        "sku_weight_kg",
        F.round(F.percent_rank().over(w_moved_at_year_by_unit_price), 3).alias("unit_price_percent_rank"),
        F.sum("sku_weight_kg").over(w_all_by_sku_weight_kg_prior_3).alias("sku_weight_kg_sum_3_prior"),
        F.lag("moved_at").over(w_move_type_by_sku_weight_kg).alias("prev_moved_at"),
        F.lag("unit_price").over(w_moved_at_year_by_unit_price_2).alias("prev_unit_price"),
        F.round(F.avg("unit_price").over(w_sku_id_move_type), 2).alias("sku_id_move_type_avg_unit_price"),
        F.count(F.lit(1)).over(w_sku_id_move_type).alias("rows_in_sku_id_move_type"),
        F.round(F.percent_rank().over(w_all_by_sku_weight_kg), 3).alias("sku_weight_kg_percent_rank"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.round(F.percent_rank().over(w_warehouse_id_sku_id_by_sku_w), 3).alias("sku_weight_kg_percent_rank_2"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_move_type_warehouse_id), F.lit(0)), 4).alias("sku_weight_kg_share_of_move_type_warehouse_id"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
        F.last("unit_price", ignorenulls=True).over(w_all_by_sku_weight_kg_running).alias("last_known_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_move_type)).alias("distinct_warehouse_id_in_sku_id_move_type"),
        F.round(F.avg("sku_weight_kg").over(w_moved_at_year_by_unit_price_3), 2).alias("sku_weight_kg_avg_1_prior"),
        F.round(F.col("sku_weight_kg") - F.avg("sku_weight_kg").over(w_move_type), 2).alias("sku_weight_kg_vs_move_type_avg"),
        F.rank().over(w_warehouse_id_sku_id_by_sku_w_2).alias("sku_weight_kg_rank"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.lead("sku_weight_kg_percent_rank_2").over(w_sku_weight_kg_rank_by_distin).alias("next_sku_weight_kg_percent_rank_2"),
        F.rank().over(w_sku_weight_kg_rank_by_distin_2).alias("distinct_warehouse_id_in_sku_id_move_type_rank"),
        F.round(F.avg("sku_weight_kg_percent_rank").over(w_sku_id), 2).alias("sku_id_avg_sku_weight_kg_percent_rank"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id)).alias("distinct_warehouse_id_in_sku_id"),
        F.rank().over(w_distinct_warehouse_id_in_sku).alias("rows_in_sku_id_move_type_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "move_type_ref",
        "sku_weight_kg",
        "sku_weight_kg_sum_3_prior",
        "prev_moved_at",
        "prev_unit_price",
        "sku_id_move_type_avg_unit_price",
        "rows_in_sku_id_move_type",
        "sku_weight_kg_percent_rank",
        "rows_in_move_type",
        "sku_weight_kg_percent_rank_2",
        "sku_weight_kg_share_of_move_type_warehouse_id",
        "unit_price_vs_move_type_avg",
        "distinct_warehouse_id_in_sku_id_move_type",
        "sku_weight_kg_avg_1_prior",
        "sku_weight_kg_vs_move_type_avg",
        "sku_weight_kg_rank",
        "next_sku_weight_kg_percent_rank_2",
        "distinct_warehouse_id_in_sku_id_move_type_rank",
        "sku_id_avg_sku_weight_kg_percent_rank",
        "distinct_warehouse_id_in_sku_id",
        F.round(F.col("unit_price_percent_rank") * 1.07, 2).alias("unit_price_percent_rank_adj"),
    )
    .orderBy(
        F.col("sku_weight_kg_vs_move_type_avg").desc(),
        F.col("sku_weight_kg_percent_rank_2").desc(),
        "id",
    )
    .limit(20)
)
