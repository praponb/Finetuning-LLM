from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_moved_at_date = Window.partitionBy("move_type").orderBy(F.col("moved_at_date").desc(), "id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id")
w_move_type_by_qty_full = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) | (F.col("operator") == "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_trends = (
    prepared_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "unit_price_per_qty",
        "moved_at_date",
        (F.col("unit_price_per_qty") - F.lag("unit_price_per_qty").over(w_move_type_by_moved_at_date)).alias("unit_price_per_qty_change"),
        F.lead("unit_price").over(w_move_type_by_qty).alias("next_unit_price"),
        F.first("moved_at").over(w_move_type_by_qty_full).alias("first_moved_at"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["move_type", "warehouse_id"], ["move_type"]],
        "move_type", "warehouse_id",
    )
    .agg(
        F.sum("unit_price_per_qty_change").alias("total_unit_price_per_qty_change"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
