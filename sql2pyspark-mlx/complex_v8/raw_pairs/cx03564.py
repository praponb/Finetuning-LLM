from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_sku_category_warehouse_city = Window.partitionBy("sku_category", "warehouse_city").orderBy(F.col("warehouse_capacity_2").desc_nulls_first(), "id")
w_sku_id_by_qty_per_unit_price = Window.partitionBy("sku_id").orderBy(F.col("qty_per_unit_price").desc(), "id")
w_sku_id_by_qty_per_unit_price_2 = Window.partitionBy("sku_id").orderBy(F.col("qty_per_unit_price").desc(), "id").rowsBetween(-3, Window.currentRow)

valid = df_stock_moves.filter((F.col("unit_price") > 205.1) & (F.col("qty") < 384))

enriched = (
    valid
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

enriched_sku = (
    joined.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg_2"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

joined_2 = (
    enriched_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(~F.array_contains(F.col("sk.labels"), "fragile"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.qty_per_unit_price",
        "s.operator_or_unknown",
        "s.warehouse_capacity",
        "s.warehouse_city",
        "s.warehouse_wh_code",
        "s.sku_code",
        "s.sku_weight_kg",
        "s.sku_weight_kg_2",
        F.col("sk.labels").alias("sku_labels_2"),
        F.col("sk.category").alias("sku_category"),
        F.col("w.capacity").alias("warehouse_capacity_2"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

with_trends = (
    joined_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "qty_per_unit_price",
        "operator_or_unknown",
        "warehouse_city",
        "warehouse_wh_code",
        "sku_weight_kg",
        "sku_labels_2",
        "sku_category",
        "warehouse_capacity_2",
        F.max("sku_weight_kg_2").over(w_move_type).alias("move_type_max_sku_weight_kg_2"),
        F.size(F.collect_set("warehouse_id").over(w_move_type)).alias("distinct_warehouse_id_in_move_type"),
        F.ntile(5).over(w_sku_category_warehouse_city).alias("warehouse_capacity_2_quintile"),
        F.round(F.percent_rank().over(w_sku_id_by_qty_per_unit_price), 3).alias("qty_per_unit_price_percent_rank"),
        F.max("warehouse_capacity_2").over(w_sku_id_by_qty_per_unit_price_2).alias("warehouse_capacity_2_max_4"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "qty_per_unit_price",
        "operator_or_unknown",
        "warehouse_city",
        "warehouse_capacity_2_quintile",
        "qty_per_unit_price_percent_rank",
        "warehouse_capacity_2_max_4",
    )
    .orderBy(F.col("warehouse_id").desc(), "move_type", "id")
    .limit(20)
)
