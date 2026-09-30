from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type_by_mo = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_move_type_by_mo_2 = Window.partitionBy("warehouse_id", "move_type").orderBy("moved_at", "id")
w_sku_id_sku_category = Window.partitionBy("sku_id", "sku_category")
w_warehouse_id_moved_at_to_dat = Window.partitionBy("warehouse_id", F.to_date("moved_at")).orderBy("qty", "id")
w_sku_category_moved_at_month = Window.partitionBy("sku_category", F.date_trunc("month", "moved_at"))
w_move_type_sku_category_by_mo = Window.partitionBy("move_type", "sku_category").orderBy("moved_at", "id")

prepared = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("unit_price") <= 396.4))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

with_details = (
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.first("moved_at").over(w_warehouse_id_move_type_by_mo).alias("first_moved_at"),
        F.lag("sku_unit_cost", 3).over(w_warehouse_id_move_type_by_mo_2).alias("prev3_sku_unit_cost"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_sku_id_sku_category), 2).alias("sku_unit_cost_vs_sku_id_sku_category_avg"),
        F.row_number().over(w_warehouse_id_move_type_by_mo_2).alias("moved_at_seq"),
        F.size(F.collect_set("move_type").over(w_sku_id_sku_category)).alias("distinct_move_type_in_sku_id_sku_category"),
        F.dense_rank().over(w_warehouse_id_moved_at_to_dat).alias("qty_dense_rank"),
        F.min("sku_unit_cost").over(w_sku_category_moved_at_month).alias("sku_category_moved_at_month_min_sku_unit_cost"),
        F.dense_rank().over(w_move_type_sku_category_by_mo).alias("moved_at_dense_rank"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("moved_at_seq") <= 3)
    .drop("moved_at_seq")
)

result = (
    top_per_group
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "sku_code",
        "sku_unit_cost",
        "first_moved_at",
        "prev3_sku_unit_cost",
        "qty_dense_rank",
        "moved_at_dense_rank",
        F.abs(F.col("qty") - 239).alias("qty_distance"),
    )
    .orderBy(F.col("id").desc(), F.col("moved_at_dense_rank").desc_nulls_first())
)
