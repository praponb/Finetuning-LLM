from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_move_qty = Window.orderBy(F.col("move_qty").asc_nulls_last(), "id", "move_id")
w_move_id_by_unit_price_minus = Window.partitionBy("move_id").orderBy(F.col("unit_price_minus_median_filled").asc_nulls_last(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_id_by_unit_price_minus_2 = Window.partitionBy("move_id").orderBy(F.col("unit_price_minus_median_filled").asc_nulls_last())

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_ann")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

detail_rows = (
    prepared_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

move_type_stats = (
    detail_rows
    .groupBy("move_type")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

compared = (
    detail_rows.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

labeled = (
    compared
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_id",
        "move_qty",
        "move_move_type",
        "move_unit_price",
        "unit_price_minus_median",
        F.coalesce(F.col("unit_price_minus_median"), F.lit(0.0)).alias("unit_price_minus_median_filled"),
        F.upper(F.trim(F.col("move_move_type"))).alias("move_move_type_clean"),
        F.abs(F.col("qty") - 299).alias("qty_distance"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "move_id",
        "move_unit_price",
        "unit_price_minus_median",
        "unit_price_minus_median_filled",
        "move_move_type_clean",
        "qty_distance",
        (F.col("unit_price_minus_median") - F.lag("unit_price_minus_median").over(w_all_by_move_qty)).alias("unit_price_minus_median_change"),
        F.nth_value("unit_price_minus_median", 2).over(w_move_id_by_unit_price_minus).alias("second_unit_price_minus_median"),
        F.dense_rank().over(w_move_id_by_unit_price_minus_2).alias("unit_price_minus_median_filled_dense_rank"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") < 384) & (F.col("st.warehouse_id").isNotNull()), "left_anti")
    .filter(F.col("move_type").isNotNull())
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.sum("unit_price").alias("total_move_unit_price"),
    )
)

with_counts = (
    qualified.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_unit_price",
    )
)

result = (
    with_counts
    .rollup("sku_id", "unit_price_minus_median_filled_dense_rank", "move_type")
    .agg(F.sum("move_unit_price").alias("total_move_unit_price_2"))
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("unit_price_minus_median_filled_dense_rank").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
