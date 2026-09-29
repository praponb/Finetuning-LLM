from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_move_type = Window.partitionBy("move_type")
w_distinct_sku_ids_sku_id_by_n = Window.partitionBy("distinct_sku_ids", "sku_id").orderBy("n_rows", "warehouse_id", "sku_id").rowsBetween(-3, Window.currentRow)
w_all_by_first_moved_at_full = Window.orderBy(F.col("first_moved_at").desc(), "warehouse_id", "sku_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_warehouse_id_by_first = Window.partitionBy("n_rows", "warehouse_id").orderBy("first_moved_at", "warehouse_id", "sku_id")
w_warehouse_id_distinct_sku_id = Window.partitionBy("warehouse_id", "distinct_sku_ids")
w_distinct_sku_ids_sku_id_by_n_2 = Window.partitionBy("distinct_sku_ids", "sku_id").orderBy("n_rows", "warehouse_id", "sku_id")

scoped_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 112.8) & (F.col("qty") > 198))
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

ranked = (
    scoped_moves
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_moved_at), 3).alias("moved_at_percent_rank"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.round(F.avg("unit_price").over(w_move_type), 2).alias("move_type_avg_unit_price"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
    )
)

rollup_base = (
    ranked
    .filter(F.col("move_type") == "OUTBOUND")
    .groupBy("warehouse_id", "sku_id")
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("n_rows") >= 3)
)

ranked_2 = (
    rollup_base
    .select(
        "*",
        F.round(F.avg("distinct_sku_ids").over(w_distinct_sku_ids_sku_id_by_n), 2).alias("distinct_sku_ids_avg_4"),
        F.nth_value("n_rows", 2).over(w_all_by_first_moved_at_full).alias("second_n_rows"),
        F.ntile(10).over(w_n_rows_warehouse_id_by_first).alias("first_moved_at_decile"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id_distinct_sku_id)).alias("distinct_sku_id_in_warehouse_id_distinct_sku_ids"),
        F.rank().over(w_distinct_sku_ids_sku_id_by_n_2).alias("n_rows_rank"),
    )
)

detail_rows_moves = (
    ranked_2.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.warehouse_id",
        "s.sku_id",
        "s.n_rows",
        "s.avg_unit_price",
        "s.second_n_rows",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    detail_rows_moves
    .groupingSets(
        [["sku_id", "warehouse_id", "move_id"], ["sku_id"], []],
        "sku_id", "warehouse_id", "move_id",
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_id").asc_nulls_last(),
    )
)
