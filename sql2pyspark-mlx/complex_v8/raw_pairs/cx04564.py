from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_warehouse_id_moved_at_year_b = Window.partitionBy("warehouse_id", F.year("moved_at")).orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").asc_nulls_last())
w_all_by_unit_price_ties = Window.orderBy("unit_price")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_move_type_by_moved_at = Window.partitionBy("sku_id", "move_type").orderBy(F.col("moved_at").desc(), "id")

recent = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 5) & (F.col("unit_price") >= 215.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.max("unit_price").over(w_move_type).alias("move_type_max_unit_price"),
        F.size(F.collect_set("warehouse_id").over(w_move_type_operator)).alias("distinct_warehouse_id_in_move_type_operator"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_operator), 2).alias("unit_price_vs_move_type_operator_avg"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
        F.lag("unit_price").over(w_warehouse_id_moved_at_year_b).alias("prev_unit_price"),
        F.dense_rank().over(w_warehouse_id_by_moved_at_tie).alias("moved_at_dense_rank"),
        F.rank().over(w_all_by_unit_price_ties).alias("unit_price_rank"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.row_number().over(w_sku_id_move_type_by_moved_at).alias("moved_at_seq"),
    )
)

grouped = (
    sequenced
    .groupBy("unit_price_rank", "warehouse_id")
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.sum(F.when(F.col("move_type") == "OUTBOUND", 1).otherwise(0)).alias("n_move_type_outbound"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.min("move_type_max_unit_price").alias("min_move_type_max_unit_price"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    grouped
    .select(
        "unit_price_rank",
        "warehouse_id",
        "move_type_list",
        "distinct_sku_ids",
        "distinct_warehouse_ids",
        F.round(F.col("distinct_sku_ids") / F.nullif(F.col("distinct_warehouse_ids"), F.lit(0)), 3).alias("distinct_sku_ids_per_distinct_warehouse_ids"),
    )
    .orderBy("unit_price_rank", "warehouse_id")
    .limit(5)
)
