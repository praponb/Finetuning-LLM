from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_move_type_le = Window.partitionBy("warehouse_id").orderBy("move_type_len", "id")
w_warehouse_id_by_move_type_le_2 = Window.partitionBy("warehouse_id").orderBy("move_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_move_type_le_3 = Window.partitionBy("warehouse_id").orderBy("move_type_len")
w_warehouse_id_by_move_type_le_4 = Window.partitionBy("warehouse_id").orderBy("move_type_len", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_stock_moves
    .filter((F.col("unit_price") <= 367.7) & (F.col("operator").isNotNull()))
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
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.row_number().over(w_warehouse_id_by_move_type_le).alias("seq_no"),
        F.max("move_type_len").over(w_warehouse_id_by_move_type_le_2).alias("max_move_type_len_so_far"),
        F.ntile(4).over(w_warehouse_id_by_move_type_le).alias("move_type_len_quartile"),
        F.round(F.percent_rank().over(w_warehouse_id_by_move_type_le_3), 3).alias("move_type_len_percent_rank"),
        F.sum("move_type_len").over(w_warehouse_id_by_move_type_le_4).alias("move_type_len_sum_4"),
    )
)

result = (
    with_trends
    .groupBy("move_type_len_quartile")
    .pivot("move_type", ["OUTBOUND", "ADJUST"])
    .agg(F.sum("max_move_type_len_so_far"))
    .orderBy("move_type_len_quartile")
)
