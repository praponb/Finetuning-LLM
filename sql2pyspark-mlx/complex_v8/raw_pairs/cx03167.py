from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_all_by_unit_price_full = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_max_move_qty_ru = Window.partitionBy("move_type").orderBy(F.col("max_move_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_max_move_qty = Window.partitionBy("move_type").orderBy(F.col("max_move_qty").desc(), "id")
w_sku_id_unit_price_rank = Window.partitionBy("sku_id", "unit_price_rank")

cleaned = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) | (F.col("unit_price") > 61.2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.max("qty").alias("max_move_qty"))
)

with_counts = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.max_move_qty")
)

filtered = (
    with_counts.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isNotNull()) & (F.col("st.qty") < 387), "left_semi")
)

with_windows = (
    filtered
    .select(
        "*",
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.nth_value("unit_price", 2).over(w_all_by_unit_price_full).alias("second_unit_price"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "unit_price",
        "unit_price_rank",
        "second_unit_price",
        F.max("unit_price").over(w_move_type_by_max_move_qty_ru).alias("max_unit_price_so_far"),
        F.dense_rank().over(w_move_type_by_max_move_qty).alias("max_move_qty_dense_rank"),
        F.max("qty").over(w_sku_id_unit_price_rank).alias("sku_id_unit_price_rank_max_qty"),
        F.lead("second_unit_price", 2, 0).over(w_move_type_by_max_move_qty).alias("next2_second_unit_price"),
        F.min("qty").over(w_sku_id_unit_price_rank).alias("sku_id_unit_price_rank_min_qty"),
        F.row_number().over(w_move_type_by_max_move_qty).alias("max_move_qty_seq"),
    )
)

joined = (
    windowed.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
    )
)

result = (
    joined
    .select(
        "id",
        "warehouse_id",
        "qty",
        "unit_price",
        "unit_price_rank",
        "second_unit_price",
        "max_unit_price_so_far",
        "max_move_qty_dense_rank",
        "sku_id_unit_price_rank_max_qty",
        "next2_second_unit_price",
        "sku_id_unit_price_rank_min_qty",
        "max_move_qty_seq",
        "warehouse_capacity",
        "sku_category",
        F.round(F.col("unit_price") / F.nullif(F.col("next2_second_unit_price"), F.lit(0)), 3).alias("unit_price_per_next2_second_unit_price"),
    )
    .orderBy(F.col("id").desc(), F.col("max_unit_price_so_far").desc())
)
