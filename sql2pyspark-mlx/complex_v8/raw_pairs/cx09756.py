from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_moved_at_to_date_warehouse_i = Window.partitionBy(F.to_date("moved_at"), "warehouse_id").orderBy("moved_at")
w_moved_at_to_date_warehouse_i_2 = Window.partitionBy(F.to_date("moved_at"), "warehouse_id").orderBy("moved_at", "id")

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type") != "TRANSFER") & (F.col("qty") <= 314))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

with_trends = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        F.round(F.col("unit_price_per_qty") / F.nullif(F.sum("unit_price_per_qty").over(w_move_type), F.lit(0)), 4).alias("unit_price_per_qty_share_of_move_type"),
        F.round(F.percent_rank().over(w_moved_at_to_date_warehouse_i), 3).alias("moved_at_percent_rank"),
        F.dense_rank().over(w_moved_at_to_date_warehouse_i_2).alias("moved_at_dense_rank"),
        F.rank().over(w_moved_at_to_date_warehouse_i).alias("moved_at_rank"),
    )
)

scored = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price_per_qty_share_of_move_type",
        "moved_at_percent_rank",
        "moved_at_dense_rank",
        "moved_at_rank",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.round(F.col("moved_at_dense_rank") / F.nullif(F.col("moved_at_rank"), F.lit(0)), 3).alias("moved_at_dense_rank_per_moved_at_rank"),
    )
)

result = (
    scored
    .filter(F.col("moved_at_percent_rank") > 0.2)
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price_per_qty_share_of_move_type",
        "moved_at_dense_rank",
        "move_type_len",
        "move_type_prefix",
        "moved_at_dense_rank_per_moved_at_rank",
    )
    .orderBy(F.col("moved_at_dense_rank").desc())
)
