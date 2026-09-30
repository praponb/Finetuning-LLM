from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_count_2_warehouse_id = Window.partitionBy("move_count_2", "warehouse_id")
w_all_by_total_move_qty = Window.orderBy(F.col("total_move_qty").desc(), "id")
w_all_by_total_move_qty_runnin = Window.orderBy(F.col("total_move_qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_move_count_full = Window.orderBy(F.col("move_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_eve")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.when(F.col("move_type").isin("ADJUST", "OUTBOUND"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.abs(F.col("qty") - 239).alias("qty_distance"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.sum("qty").alias("total_move_qty"),
        F.count("*").alias("move_count_2"),
    )
)

summarized_rows = (
    valid_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_qty",
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

metrics = (
    summarized_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "move_type_group",
        "qty_per_unit_price",
        "qty_distance",
        "move_count",
        "total_move_qty",
        "move_count_2",
        F.round(F.col("move_count_2") / F.nullif(F.sum("move_count_2").over(w_move_count_2_warehouse_id), F.lit(0)), 4).alias("move_count_2_share_of_move_count_2_warehouse_id"),
        F.round(F.col("qty_per_unit_price") / F.nullif(F.sum("qty_per_unit_price").over(w_move_count_2_warehouse_id), F.lit(0)), 4).alias("qty_per_unit_price_share_of_move_count_2_warehou"),
        F.lead("qty_distance").over(w_all_by_total_move_qty).alias("next_qty_distance"),
        F.last("move_count_2", ignorenulls=True).over(w_all_by_total_move_qty_runnin).alias("last_known_move_count_2"),
        F.first("move_count").over(w_all_by_move_count_full).alias("first_move_count"),
        F.row_number().over(w_all_by_total_move_qty).alias("seq_no"),
    )
)

per_group = (
    metrics
    .groupBy("move_count", "warehouse_id")
    .agg(
        F.min("next_qty_distance").alias("min_next_qty_distance"),
        F.round(100 * F.count(F.when(F.col("operator") == "op_eve", 1)) / F.count("*"), 1).alias("pct_operator_op_eve"),
        F.avg("qty_per_unit_price_share_of_move_count_2_warehou").alias("avg_qty_per_unit_price_share_of_move_count_2_war"),
    )
    .filter(F.col("avg_qty_per_unit_price_share_of_move_count_2_war") > 0.2)
    .drop("avg_qty_per_unit_price_share_of_move_count_2_war")
)

result = (
    per_group
    .select(
        "move_count",
        "warehouse_id",
        "min_next_qty_distance",
        (F.floor(F.col("min_next_qty_distance") / 100) * 100).alias("min_next_qty_distance_bucket"),
    )
    .orderBy(F.col("warehouse_id").desc(), F.col("min_next_qty_distance").desc())
)
