from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at = Window.orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_operator_len_warehouse_id = Window.partitionBy("operator_len", "warehouse_id")
w_warehouse_id_move_type_by_op = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("operator_len").desc(), "id")
w_operator_len_qty_tier_by_war = Window.partitionBy("operator_len", "qty_tier").orderBy(F.col("warehouse_opened_on").desc())
w_move_type_operator_len = Window.partitionBy("move_type", "operator_len")
w_operator_len_move_type_by_wa = Window.partitionBy("operator_len", "move_type").orderBy(F.col("warehouse_opened_on_percent_rank").desc())
w_qty_tier_by_moved_at_ties = Window.partitionBy("qty_tier").orderBy("moved_at")
w_operator_len_move_type_by_wa_2 = Window.partitionBy("operator_len", "move_type").orderBy(F.col("warehouse_opened_on_percent_rank").desc(), "id")

base_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 4) & (F.col("unit_price") < 317.0))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.when(F.col("qty") >= 392, "A").when(F.col("qty") >= 169, "B").otherwise("C").alias("qty_tier"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

with_details_warehouse = (
    base_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on") >= F.lit("2024-04-13").cast("date")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.qty_tier",
        "s.operator_len",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

ranked = (
    with_details_warehouse
    .select(
        "*",
        F.dense_rank().over(w_all_by_moved_at).alias("moved_at_dense_rank"),
        F.sum("unit_price").over(w_operator_len_warehouse_id).alias("operator_len_warehouse_id_sum_unit_price"),
        F.round(F.avg("operator_len").over(w_operator_len_warehouse_id), 2).alias("operator_len_warehouse_id_avg_operator_len"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_operator_len_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_operator_len_warehouse_id"),
        F.max("qty").over(w_operator_len_warehouse_id).alias("operator_len_warehouse_id_max_qty"),
        F.lag("operator_len", 2).over(w_warehouse_id_move_type_by_op).alias("prev2_operator_len"),
        F.round(F.percent_rank().over(w_operator_len_qty_tier_by_war), 3).alias("warehouse_opened_on_percent_rank"),
        F.round(F.avg("unit_price").over(w_operator_len_warehouse_id), 2).alias("operator_len_warehouse_id_avg_unit_price"),
        F.round(F.col("operator_len") / F.nullif(F.sum("operator_len").over(w_operator_len_warehouse_id), F.lit(0)), 4).alias("operator_len_share_of_operator_len_warehouse_id"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.round(F.avg("prev2_operator_len").over(w_move_type_operator_len), 2).alias("move_type_operator_len_avg_prev2_operator_len"),
        F.dense_rank().over(w_operator_len_move_type_by_wa).alias("warehouse_opened_on_percent_rank_dense_rank"),
        F.dense_rank().over(w_qty_tier_by_moved_at_ties).alias("moved_at_dense_rank_2"),
        F.ntile(10).over(w_operator_len_move_type_by_wa_2).alias("warehouse_opened_on_percent_rank_decile"),
    )
)

result = (
    metrics
    .filter(F.col("qty_tier").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "qty_tier",
        "operator_len",
        "warehouse_opened_on",
        "moved_at_dense_rank",
        "operator_len_warehouse_id_sum_unit_price",
        "operator_len_warehouse_id_avg_operator_len",
        "unit_price_share_of_operator_len_warehouse_id",
        "prev2_operator_len",
        "move_type_operator_len_avg_prev2_operator_len",
        "warehouse_opened_on_percent_rank_dense_rank",
        "moved_at_dense_rank_2",
        "warehouse_opened_on_percent_rank_decile",
        F.when(F.col("qty") >= 348, "HIGH").when(F.col("qty") >= 123, "MEDIUM").otherwise("LOW").alias("qty_tier_2"),
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
    )
    .orderBy("prev2_operator_len", F.col("id").desc())
)
