from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_by_qty_ties = Window.partitionBy("operator").orderBy("qty")
w_operator_warehouse_id_by_uni = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_moved_at = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_by_qty = Window.partitionBy("operator").orderBy("qty", "id")

recent = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
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

with_windows = (
    recent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id"),
        F.dense_rank().over(w_operator_by_qty_ties).alias("qty_dense_rank"),
        F.nth_value("moved_at", 2).over(w_operator_warehouse_id_by_uni).alias("second_moved_at"),
        F.ntile(4).over(w_operator_by_moved_at).alias("moved_at_quartile"),
        F.lag("moved_at", 2).over(w_operator_by_qty).alias("prev2_moved_at"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id), 2).alias("qty_vs_warehouse_id_avg"),
        F.nth_value("qty", 2).over(w_operator_warehouse_id_by_uni).alias("second_qty"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.max("unit_price").over(w_warehouse_id).alias("warehouse_id_max_unit_price"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("qty_dense_rank") <= 3) & (F.col("qty_vs_warehouse_id_avg") >= -200.0))
)

result = (
    top_per_group
    .filter(F.col("second_qty") > 109)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        "unit_price_share_of_warehouse_id",
        "qty_dense_rank",
        "second_moved_at",
        "moved_at_quartile",
        "prev2_moved_at",
        "qty_vs_warehouse_id_avg",
        "second_qty",
        "rows_in_warehouse_id",
        "warehouse_id_max_unit_price",
        F.when(F.col("move_type").isin("ADJUST", "INBOUND"), "group_a").when(F.col("move_type") == "TRANSFER", "group_b").otherwise("other").alias("move_type_group"),
    )
    .orderBy(
        F.col("moved_at_quartile").desc_nulls_first(),
        F.col("move_type_group").asc(),
        "id",
    )
    .limit(5)
)
