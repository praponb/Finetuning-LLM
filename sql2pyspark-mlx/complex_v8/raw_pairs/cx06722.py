from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id_by_le = Window.partitionBy("move_type", "warehouse_id").orderBy("least_unit_price_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_warehouse_id_by_le_2 = Window.partitionBy("move_type", "warehouse_id").orderBy("least_unit_price_qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-02-12 11:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 292.3, "large").when(F.col("unit_price") >= 238.5, "medium").otherwise("small").alias("unit_price_tier"),
        F.substring(F.trim(F.col("operator")), 1, 2).alias("operator_prefix"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

metrics = (
    filtered_moves
    .select(
        "*",
        F.last("unit_price", ignorenulls=True).over(w_move_type_warehouse_id_by_le).alias("last_known_unit_price"),
        F.nth_value("moved_at", 2).over(w_move_type_warehouse_id_by_le_2).alias("second_moved_at"),
    )
)

tagged = (
    metrics
    .filter(F.col("second_moved_at") < F.lit("2025-02-12 13:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat(F.col("unit_price_tier"), F.lit("#"), F.col("sku_id").cast("string")).alias("unit_price_tier_ref"),
        F.concat_ws(": ", F.col("move_type"), F.col("unit_price_tier")).alias("move_type_unit_price_tier_label"),
        F.round((F.unix_timestamp(F.col("second_moved_at")) - F.unix_timestamp(F.col("moved_at"))) / 60, 1).alias("minutes_moved_at_to_second_moved_at"),
    )
)

result = (
    tagged
    .groupingSets(
        [["move_type", "sku_id"], ["move_type"]],
        "move_type", "sku_id",
    )
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.sum("qty").alias("total_qty"),
        F.max("last_known_unit_price").alias("max_last_known_unit_price"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "sku_id",
        "first_moved_at",
        "total_qty",
        "max_last_known_unit_price",
    )
    .orderBy(F.col("move_type").asc_nulls_last(), F.col("sku_id").asc_nulls_last())
)
