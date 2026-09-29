from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_opened_on = Window.orderBy(F.col("warehouse_opened_on").desc(), "id")
w_warehouse_city = Window.partitionBy("warehouse_city")

base_moves = (
    df_stock_moves
    .filter(F.col("qty") < 332)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.abs(F.col("unit_price") - 215.4).alias("unit_price_distance"),
    )
)

combined_details = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.upper(F.trim(F.col("sk.sku_code"))).like("T%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_details_warehouse = (
    combined_details.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.wh_code").isNotNull()), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.sku_unit_cost",
        "s.warehouse_opened_on",
        "s.warehouse_capacity",
        F.col("w.opened_on").alias("warehouse_opened_on_2"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

sequenced = (
    with_details_warehouse
    .withColumn("prev_sku_weight_kg", F.lag("sku_weight_kg").over(w_all_by_warehouse_opened_on))
    .withColumn("rows_in_warehouse_city", F.count(F.lit(1)).over(w_warehouse_city))
)

labeled = (
    sequenced
    .select(
        "*",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        (F.floor(F.col("prev_sku_weight_kg") / 5) * 5).alias("prev_sku_weight_kg_bucket"),
    )
)

result = (
    labeled
    .select("warehouse_capacity")
    .distinct()
    .orderBy(F.col("warehouse_capacity").desc())
)
