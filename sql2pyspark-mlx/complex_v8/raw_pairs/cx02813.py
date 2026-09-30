from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_len_move_type_by_m = Window.partitionBy("move_type_len", "move_type").orderBy(F.col("move_type_len").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")

recent_moves = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date"))
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

metrics = (
    recent_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_len",
        F.sum("unit_price").over(w_move_type_len_move_type_by_m).alias("running_unit_price"),
        F.max("qty").over(w_warehouse_id).alias("warehouse_id_max_qty"),
        F.last("unit_price", ignorenulls=True).over(w_move_type_len_move_type_by_m).alias("last_known_unit_price"),
        F.round(F.avg("move_type_len").over(w_warehouse_id), 2).alias("warehouse_id_avg_move_type_len"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "operator",
        "move_type_len",
        "running_unit_price",
        "last_known_unit_price",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

result = (
    annotated
    .groupBy("moved_at")
    .pivot("operator", ["op_dew", "op_ann", "op_bee"])
    .agg(F.sum("unit_price_adj"))
    .orderBy("moved_at")
)
