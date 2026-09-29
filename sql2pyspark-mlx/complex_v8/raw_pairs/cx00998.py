from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_all_by_sku_weight_kg_running = Window.orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_sku_weight_kg_full = Window.orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_sku_weight_kg = Window.orderBy("sku_weight_kg", "id")

scoped_moves = (
    df_stock_moves
    .filter(F.col("operator") != "op_eve")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

scored = (
    scoped_moves
    .filter(F.col("unit_price") <= 361.9)
    .select(
        "*",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

combined_details_sku = (
    scored.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_filled",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

tagged = (
    combined_details_sku
    .select(
        "*",
        F.when(F.col("unit_price_filled") >= 375.2, "HIGH").when(F.col("unit_price_filled") >= 175.7, "MEDIUM").otherwise("LOW").alias("unit_price_filled_tier"),
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "unit_price",
        "unit_price_filled",
        "sku_weight_kg",
        "sku_is_active",
        "moved_at_plus_30m",
        F.round(F.avg("unit_price_filled").over(w_move_type), 2).alias("move_type_avg_unit_price_filled"),
        F.max("unit_price_filled").over(w_all_by_sku_weight_kg_running).alias("max_unit_price_filled_so_far"),
        F.first("unit_price").over(w_all_by_sku_weight_kg_full).alias("first_unit_price"),
        F.min("unit_price").over(w_move_type).alias("move_type_min_unit_price"),
        F.lead("moved_at_plus_30m").over(w_all_by_sku_weight_kg).alias("next_moved_at_plus_30m"),
        F.ntile(4).over(w_all_by_sku_weight_kg).alias("sku_weight_kg_quartile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price_filled",
        "sku_weight_kg",
        "moved_at_plus_30m",
        "move_type_avg_unit_price_filled",
        "max_unit_price_filled_so_far",
        "next_moved_at_plus_30m",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        (F.floor(F.col("sku_weight_kg") / 5) * 5).alias("sku_weight_kg_bucket"),
    )
    .orderBy("move_type_slug", F.col("max_unit_price_filled_so_far").desc())
)
