from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_category").orderBy("qty", "id")
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy("qty", "id")
w_all_by_unit_price_running = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy("unit_price", "id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("qty") <= 387)
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
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        F.col("sk.category").alias("sku_category"),
        "sk.sku_code",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

with_activity = (
    with_parent.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.is_active")) & (F.col("sk.unit_cost") <= 251.2), "left_semi")
)

unique_rows = (
    with_activity
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    unique_rows
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "sku_category",
        "sku_code",
        "sku_is_active",
        F.ntile(4).over(w_sku_id_by_qty).alias("qty_quartile"),
        F.first("moved_at", ignorenulls=True).over(w_all_by_unit_price_running).alias("first_known_moved_at"),
        F.dense_rank().over(w_warehouse_id_by_unit_price).alias("unit_price_dense_rank"),
        (F.col("unit_price") - F.lag("unit_price").over(w_warehouse_id_by_unit_price)).alias("unit_price_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "move_type",
        "sku_category",
        "sku_is_active",
        "first_known_moved_at",
        "unit_price_dense_rank",
        "unit_price_change",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
    )
    .orderBy(F.col("move_type").desc())
)
