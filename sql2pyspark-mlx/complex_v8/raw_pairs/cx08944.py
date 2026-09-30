from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_sku_id_by_qty_full = Window.partitionBy("sku_id").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_stock_moves
    .filter((F.col("unit_price") < 399.6) & (F.col("sku_id") >= 8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
    )
)

joined_sku = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at_label",
        F.col("sk.is_active").alias("sku_is_active"),
        "sk.sku_code",
    )
)

windowed = (
    joined_sku
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at_label",
        "sku_is_active",
        F.size(F.collect_set("warehouse_id").over(w_move_type)).alias("distinct_warehouse_id_in_move_type"),
        F.nth_value("qty", 2).over(w_sku_id_by_qty_full).alias("second_qty"),
    )
)

result = (
    windowed
    .filter(~F.col("sku_is_active"))
    .select(
        "id",
        "warehouse_id",
        "sku_is_active",
        "distinct_warehouse_id_in_move_type",
        "second_qty",
        F.round(F.col("qty") / F.nullif(F.col("second_qty"), F.lit(0)), 3).alias("qty_per_second_qty"),
        F.greatest(F.col("second_qty"), F.col("qty")).alias("greatest_second_qty_qty"),
    )
    .orderBy(F.col("qty_per_second_qty").desc_nulls_first())
)
