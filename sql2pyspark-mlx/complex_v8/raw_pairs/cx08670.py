from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy(F.col("moved_at").asc_nulls_last(), "id")
w_sku_id_by_qty_moving_1 = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(-1, Window.currentRow)
w_sku_id_move_type_by_moved_at_2 = Window.partitionBy("sku_id", "move_type").orderBy(F.col("moved_at").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy("qty", "id")
w_sku_id_by_qty_ties = Window.partitionBy("sku_id").orderBy("qty")
w_latest = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("qty_dense_rank").desc(), F.col("id").desc())

recent_moves = (
    df_stock_moves
    .filter((F.col("unit_price") < 299.1) & (F.col("qty") >= 116))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
    )
)

with_windows = (
    recent_moves
    .select(
        "*",
        F.round(F.percent_rank().over(w_sku_id_move_type_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.sum("unit_price_adj").over(w_sku_id_by_qty_moving_1).alias("unit_price_adj_sum_2"),
        F.round(F.avg("qty").over(w_sku_id_move_type_by_moved_at_2), 2).alias("qty_avg_3_prior"),
        F.row_number().over(w_sku_id_by_qty).alias("seq_no"),
        F.dense_rank().over(w_sku_id_by_qty_ties).alias("qty_dense_rank"),
        F.ntile(3).over(w_sku_id_move_type_by_moved_at).alias("moved_at_tercile"),
    )
)

unique_rows = (
    with_windows
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

line_level = (
    unique_rows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.moved_at_percent_rank",
        "s.qty_avg_3_prior",
        "s.qty_dense_rank",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
    )
)

filtered = (
    line_level.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") < 273.5), "left_anti")
)

result = (
    filtered
    .filter(F.col("move_qty") >= 67)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "qty_dense_rank",
        "move_moved_at",
        "move_qty",
        F.length(F.trim(F.col("move_move_type"))).alias("move_move_type_len"),
        F.coalesce(F.col("move_operator"), F.lit("UNKNOWN")).alias("move_operator_or_unknown"),
    )
    .orderBy(F.col("moved_at").desc())
)
