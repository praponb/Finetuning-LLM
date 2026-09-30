from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_warehouse_capaci = Window.partitionBy("operator").orderBy(F.col("warehouse_capacity").desc_nulls_first(), "id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_sku_id_move_type_by_warehous = Window.partitionBy("sku_id", "move_type").orderBy("warehouse_opened_on", "id")
w_operator_by_unit_price_runni = Window.partitionBy("operator").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_by_unit_price_movi = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_warehouse_capacity_ru = Window.orderBy(F.col("warehouse_capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_warehouse_capaci_2 = Window.partitionBy("operator").orderBy(F.col("warehouse_capacity").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_by_unit_price_ties = Window.partitionBy("operator").orderBy("unit_price")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("sku_id") <= 7) & (F.col("warehouse_id").isNotNull()))
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

enriched = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w_operator_by_warehouse_capaci).alias("warehouse_capacity_quartile"),
        F.round(F.col("sku_weight_kg") - F.avg("sku_weight_kg").over(w_warehouse_id_sku_id), 2).alias("sku_weight_kg_vs_warehouse_id_sku_id_avg"),
        F.min("qty").over(w_warehouse_id_operator).alias("warehouse_id_operator_min_qty"),
        F.ntile(4).over(w_sku_id_move_type_by_warehous).alias("warehouse_opened_on_quartile"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_id_operator), F.lit(0)), 4).alias("qty_share_of_warehouse_id_operator"),
        F.sum("qty").over(w_warehouse_id_operator).alias("warehouse_id_operator_sum_qty"),
        F.lag("unit_price").over(w_sku_id_move_type_by_warehous).alias("prev_unit_price"),
        F.min("unit_price").over(w_warehouse_id_operator).alias("warehouse_id_operator_min_unit_price"),
        F.min("unit_price").over(w_operator_by_unit_price_runni).alias("min_unit_price_so_far"),
        F.lag("warehouse_capacity").over(w_operator_by_warehouse_capaci).alias("prev_warehouse_capacity"),
        F.max("unit_price").over(w_move_type_by_unit_price_movi).alias("unit_price_max_3"),
        F.size(F.collect_set("operator").over(w_warehouse_id_sku_id)).alias("distinct_operator_in_warehouse_id_sku_id"),
        F.last("qty", ignorenulls=True).over(w_all_by_warehouse_capacity_ru).alias("last_known_qty"),
        F.first("unit_price").over(w_operator_by_warehouse_capaci_2).alias("first_unit_price"),
        F.dense_rank().over(w_operator_by_unit_price_ties).alias("unit_price_dense_rank"),
    )
)

enriched_2 = (
    with_trends
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "sku_weight_kg",
        "sku_category",
        "sku_unit_cost",
        "warehouse_opened_on",
        "sku_weight_kg_vs_warehouse_id_sku_id_avg",
        "warehouse_id_operator_min_qty",
        "warehouse_opened_on_quartile",
        "prev_unit_price",
        "min_unit_price_so_far",
        "distinct_operator_in_warehouse_id_sku_id",
        "last_known_qty",
        "first_unit_price",
        "unit_price_dense_rank",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

result = (
    enriched_2
    .filter(F.col("sku_unit_cost") > 164.5)
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "sku_weight_kg",
        "sku_category",
        "sku_unit_cost",
        "warehouse_opened_on",
        "sku_weight_kg_vs_warehouse_id_sku_id_avg",
        "warehouse_id_operator_min_qty",
        "prev_unit_price",
        "min_unit_price_so_far",
        "last_known_qty",
        "first_unit_price",
        "unit_price_dense_rank",
        "moved_at_plus_30m",
        "moved_at_label",
        F.abs(F.col("first_unit_price") - 255.6).alias("first_unit_price_distance"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
    .orderBy(
        F.col("first_unit_price").desc(),
        F.col("prev_unit_price").asc_nulls_last(),
    )
)
