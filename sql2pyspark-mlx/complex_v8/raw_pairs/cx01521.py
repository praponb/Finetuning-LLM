from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("qty") < 372) & (F.col("operator") != "op_bee"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
    )
)

line_level = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

with_counts = (
    line_level.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.last_moved_at")
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

profiled = (
    with_counts.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.move_warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

with_children = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_id",
        "s.move_move_type",
        "s.move_moved_at",
        "s.move_warehouse_id",
        "s.last_moved_at",
        F.col("st.id").alias("move_id_2"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at_2"),
        F.col("st.move_type").alias("move_move_type_2"),
    )
)

joined_move = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id_2"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_id",
        "s.move_moved_at",
        "s.move_warehouse_id",
        "s.last_moved_at",
        "s.move_id_2",
        "s.move_moved_at_2",
        F.col("st.moved_at").alias("move_moved_at_3"),
    )
)

with_details_move = (
    joined_move.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .filter(F.col("st.qty") < 275)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.move_id",
        "s.move_moved_at",
        "s.move_warehouse_id",
        "s.last_moved_at",
        "s.move_id_2",
        "s.move_moved_at_2",
        "s.move_moved_at_3",
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

result = (
    with_details_move
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "move_id",
        "move_moved_at",
        "move_warehouse_id",
        "last_moved_at",
        "move_id_2",
        "move_moved_at_2",
        "move_moved_at_3",
        "move_qty",
        F.when(F.col("move_qty") >= 355, "HIGH").when(F.col("move_qty") >= 124, "MEDIUM").otherwise("LOW").alias("move_qty_tier"),
        (F.floor(F.col("move_qty") / 100) * 100).alias("move_qty_bucket"),
    )
    .orderBy("move_id")
)
