from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_capacity = Window.orderBy(F.col("warehouse_capacity").desc(), "id")
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy("unit_price", "id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_move_type_by_unit_price_runn = Window.partitionBy("move_type").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_moves = df_stock_moves.filter((F.col("sku_id").isNotNull()) & (F.col("move_type") != "TRANSFER"))

with_details = (
    base_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.capacity") > 4912)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "sku_labels",
        F.lag("warehouse_capacity").over(w_all_by_warehouse_capacity).alias("prev_warehouse_capacity"),
        F.row_number().over(w_move_type_by_unit_price).alias("rn"),
        F.lag("unit_price", 2).over(w_all_by_unit_price).alias("prev2_unit_price"),
        F.first("moved_at", ignorenulls=True).over(w_move_type_by_unit_price_runn).alias("first_known_moved_at"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["warehouse_id", "sku_id"], ["warehouse_id"], ["sku_id"]],
        "warehouse_id", "sku_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("prev2_unit_price"), 2).alias("stddev_prev2_unit_price"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
