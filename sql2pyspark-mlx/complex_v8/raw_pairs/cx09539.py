from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_move_count_prior_6 = Window.orderBy("move_count", "id").rowsBetween(-6, -1)
w_move_count = Window.partitionBy("move_count")
w_all_by_move_count = Window.orderBy("move_count", "id")
w_all_by_move_count_ties = Window.orderBy("move_count")

scoped_moves = (
    df_stock_moves
    .filter((F.col("unit_price") <= 307.8) & (F.col("warehouse_id") >= 3))
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

with_parent = (
    scoped_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("move_move_type_count"),
        F.count("*").alias("move_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
    )
)

with_counts = (
    with_parent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.avg_move_unit_price",
    )
)

windowed = (
    with_counts
    .select(
        "*",
        F.max("qty").over(w_all_by_move_count_prior_6).alias("qty_max_6_prior"),
        F.min("warehouse_capacity").over(w_move_count).alias("move_count_min_warehouse_capacity"),
        F.row_number().over(w_all_by_move_count).alias("seq_no"),
        F.dense_rank().over(w_all_by_move_count_ties).alias("move_count_dense_rank"),
        F.count(F.lit(1)).over(w_move_count).alias("rows_in_move_count"),
    )
)

leaders = windowed.filter(F.col("seq_no") <= 3)

result = (
    leaders
    .groupBy("warehouse_city", "move_count")
    .pivot("sku_category", ["Toys", "Beauty"])
    .agg(F.sum("warehouse_capacity"))
)
