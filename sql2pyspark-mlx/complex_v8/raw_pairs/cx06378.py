from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_category_warehouse_id_by = Window.partitionBy("sku_category", "warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_category_warehouse_id_by_2 = Window.partitionBy("sku_category", "warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_category_by_qty_distance = Window.partitionBy("sku_category").orderBy("qty_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_category = Window.partitionBy("sku_category")
w_sku_category_operator_by_sku = Window.partitionBy("sku_category", "operator").orderBy("sku_weight_kg", "id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_unit_price_full = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.abs(F.col("qty") - 348).alias("qty_distance"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.move_type_clean",
        "s.qty_distance",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.dense_rank().over(w_sku_category_warehouse_id_by).alias("unit_price_dense_rank"),
        F.row_number().over(w_sku_category_warehouse_id_by).alias("unit_price_seq"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_warehouse_id), F.lit(0)), 4).alias("sku_weight_kg_share_of_warehouse_id"),
        F.nth_value("qty_distance", 2).over(w_sku_category_warehouse_id_by_2).alias("second_qty_distance"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.first("unit_price", ignorenulls=True).over(w_sku_category_by_qty_distance).alias("first_known_unit_price"),
        F.lag("unit_price", 3).over(w_sku_category_warehouse_id_by).alias("prev3_unit_price"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
    )
)

top_rows = (
    windowed
    .filter(F.col("unit_price_dense_rank") == 1)
    .drop("unit_price_dense_rank")
)

with_windows = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "move_type_clean",
        "sku_weight_kg",
        "sku_category",
        "unit_price_seq",
        "sku_weight_kg_share_of_warehouse_id",
        "second_qty_distance",
        "prev3_unit_price",
        "distinct_sku_id_in_warehouse_id",
        F.round(F.avg("unit_price").over(w_sku_category), 2).alias("sku_category_avg_unit_price"),
        F.count(F.lit(1)).over(w_sku_category).alias("rows_in_sku_category"),
        F.round(F.percent_rank().over(w_sku_category_operator_by_sku), 3).alias("sku_weight_kg_percent_rank"),
        F.dense_rank().over(w_all_by_unit_price).alias("unit_price_dense_rank"),
        F.last("second_qty_distance").over(w_all_by_unit_price_full).alias("last_second_qty_distance"),
    )
)

result = (
    with_windows
    .filter(F.col("sku_category_avg_unit_price") > 50.9)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type_clean",
        "sku_weight_kg",
        "sku_category",
        "unit_price_seq",
        "second_qty_distance",
        "prev3_unit_price",
        "distinct_sku_id_in_warehouse_id",
        "sku_category_avg_unit_price",
        "rows_in_sku_category",
        "sku_weight_kg_percent_rank",
        "unit_price_dense_rank",
        F.abs(F.col("unit_price_seq") - 4).alias("unit_price_seq_distance"),
    )
    .orderBy("id", F.col("unit_price_seq").asc_nulls_last())
)
