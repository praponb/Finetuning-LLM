from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_unit_cost_2_tier_by_qty = Window.partitionBy("sku_unit_cost_2_tier").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), "id")

scoped = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
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

joined_sku = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
    )
)

enriched_sku = (
    joined_sku.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.weight_kg") <= 20.9)
    .select(
        "s.*",
        F.col("sk.weight_kg").alias("sku_weight_kg_2"),
        F.col("sk.unit_cost").alias("sku_unit_cost_2"),
        F.col("sk.category").alias("sku_category_2"),
    )
)

enriched = (
    enriched_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        "sku_weight_kg",
        "sku_category",
        "sku_category_2",
        F.abs(F.col("sku_weight_kg") - 12.4).alias("sku_weight_kg_distance"),
        F.when(F.col("sku_unit_cost_2") >= 251.2, "A").when(F.col("sku_unit_cost_2") >= 108.5, "B").otherwise("C").alias("sku_unit_cost_2_tier"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_sku_unit_cost_2_tier_by_qty).alias("running_count"),
        F.min("sku_weight_kg_distance").over(w_warehouse_id).alias("warehouse_id_min_sku_weight_kg_distance"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id), 2).alias("qty_vs_warehouse_id_avg"),
        F.row_number().over(w_warehouse_id_by_qty).alias("seq_no"),
        F.max("unit_price").over(w_warehouse_id).alias("warehouse_id_max_unit_price"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_warehouse_id), F.lit(0)), 4).alias("sku_weight_kg_share_of_warehouse_id"),
    )
)

result = (
    ranked
    .rollup("warehouse_id", "sku_unit_cost_2_tier", "seq_no")
    .agg(
        F.sum(F.when(F.col("sku_category_2").isin("Beauty", "Electronics", "Apparel"), 1).otherwise(0)).alias("n_sku_category_2_in_beauty_ele"),
        F.count("*").alias("cnt"),
    )
    .select(
        "warehouse_id",
        F.coalesce(F.col("sku_unit_cost_2_tier"), F.lit("ALL")).alias("sku_unit_cost_2_tier"),
        "seq_no",
        "n_sku_category_2_in_beauty_ele",
        "cnt",
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_unit_cost_2_tier").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
