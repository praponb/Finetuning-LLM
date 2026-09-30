from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type_by_qt = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("qty").desc())
w_warehouse_id_move_type_by_qt_2 = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("qty").desc(), "id")
w_all_by_qty = Window.orderBy("qty", "id")
w_qty_dense_rank = Window.partitionBy("qty_dense_rank")
w_all_by_qty_ties = Window.orderBy("qty")

recent = (
    df_stock_moves
    .filter((F.col("unit_price") >= 180.0) & (F.col("moved_at") < F.lit("2025-03-02 06:00:00").cast("timestamp")))
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

windowed = (
    recent
    .select(
        "*",
        F.rank().over(w_warehouse_id_move_type_by_qt).alias("qty_rank"),
        F.row_number().over(w_warehouse_id_move_type_by_qt_2).alias("rn"),
        F.dense_rank().over(w_warehouse_id_move_type_by_qt).alias("qty_dense_rank"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "qty_rank",
        "rn",
        "qty_dense_rank",
        F.ntile(10).over(w_all_by_qty).alias("qty_decile"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_qty_dense_rank), F.lit(0)), 4).alias("qty_share_of_qty_dense_rank"),
        F.round(F.col("qty_rank") - F.avg("qty_rank").over(w_qty_dense_rank), 2).alias("qty_rank_vs_qty_dense_rank_avg"),
        F.round(F.cume_dist().over(w_all_by_qty_ties), 3).alias("qty_cume_dist"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_qty_dense_rank), 2).alias("unit_price_vs_qty_dense_rank_avg"),
    )
)

result = (
    metrics
    .rollup("qty_dense_rank", "qty_decile")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("qty_cume_dist"), 2).alias("avg_qty_cume_dist"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.percentile_approx("qty_decile", 0.5).alias("median_qty_decile"),
    )
    .orderBy(
        F.col("qty_dense_rank").asc_nulls_last(),
        F.col("qty_decile").asc_nulls_last(),
    )
)
