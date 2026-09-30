from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty = Window.orderBy(F.col("qty").desc_nulls_first(), "id")
w_operator_sku_id_by_qty = Window.partitionBy("operator", "sku_id").orderBy("qty", "id")

scoped_moves = (
    df_stock_moves
    .filter((F.col("sku_id") >= 17) & (F.col("warehouse_id").isNotNull()))
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

with_trends = (
    scoped_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.lag("qty", 1, 0).over(w_all_by_qty).alias("prev_qty"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_operator_sku_id_by_qty))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

exploded_detail_moves = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.prev_qty",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    exploded_detail_moves
    .filter(F.col("unit_price") <= 396.4)
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price",
        "prev_qty",
        "move_id",
        "move_warehouse_id",
        "move_moved_at",
    )
    .orderBy(F.col("move_moved_at").desc(), F.col("id").desc())
)
