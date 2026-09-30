from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_category_sku_id = Window.partitionBy("sku_category", "sku_id")
w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_move_type_by_unit_price_dist = Window.partitionBy("move_type").orderBy(F.col("unit_price_distance").desc(), "id").rowsBetween(-6, Window.currentRow)
w_operator_sku_category = Window.partitionBy("operator", "sku_category")
w_move_type_by_unit_price_dist_2 = Window.partitionBy("move_type").orderBy(F.col("unit_price_distance").desc())
w_all_by_unit_price_2 = Window.orderBy("unit_price", "id")
w_warehouse_city_unit_price_qu = Window.partitionBy("warehouse_city", "unit_price_quintile")
w_sku_id_by_operator_sku_categ = Window.partitionBy("sku_id").orderBy(F.col("operator_sku_category_max_qty").asc_nulls_last(), "id")
w_sku_id_distinct_warehouse_ci = Window.partitionBy("sku_id", "distinct_warehouse_city_in_operator_sku_category").orderBy(F.col("warehouse_capacity_share_of_sku_category_sku_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_stock_moves
    .filter((F.col("move_type").isin("ADJUST", "TRANSFER")) | (F.col("qty") >= 112))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_ref"),
        F.abs(F.col("unit_price") - 248.3).alias("unit_price_distance"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.unit_price_distance",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "unit_price_distance",
        "sku_code",
        "warehouse_capacity",
        "warehouse_city",
        F.round(F.col("warehouse_capacity") / F.nullif(F.sum("warehouse_capacity").over(w_sku_category_sku_id), F.lit(0)), 4).alias("warehouse_capacity_share_of_sku_category_sku_id"),
        F.min("unit_price").over(w_all_by_unit_price_running).alias("min_unit_price_so_far"),
        F.ntile(5).over(w_all_by_unit_price).alias("unit_price_quintile"),
        F.sum("sku_weight_kg").over(w_move_type_by_unit_price_dist).alias("sku_weight_kg_sum_7"),
        F.round(F.avg("unit_price_distance").over(w_operator_sku_category), 2).alias("operator_sku_category_avg_unit_price_distance"),
        F.max("qty").over(w_operator_sku_category).alias("operator_sku_category_max_qty"),
        F.size(F.collect_set("warehouse_city").over(w_operator_sku_category)).alias("distinct_warehouse_city_in_operator_sku_category"),
        F.round(F.cume_dist().over(w_move_type_by_unit_price_dist_2), 3).alias("unit_price_distance_cume_dist"),
        F.size(F.collect_set("warehouse_city").over(w_sku_category_sku_id)).alias("distinct_warehouse_city_in_sku_category_sku_id"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator_sku_category), 2).alias("qty_vs_operator_sku_category_avg"),
        F.row_number().over(w_all_by_unit_price_2).alias("rn"),
        F.round(F.avg("unit_price").over(w_move_type_by_unit_price_dist), 2).alias("unit_price_avg_7"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("rn") <= 5)
    .drop("rn")
)

ranked = (
    top_per_group
    .select(
        "*",
        F.round(F.col("unit_price_avg_7") / F.nullif(F.sum("unit_price_avg_7").over(w_warehouse_city_unit_price_qu), F.lit(0)), 4).alias("unit_price_avg_7_share_of_warehouse_city_unit_pr"),
        F.lead("unit_price_quintile").over(w_sku_id_by_operator_sku_categ).alias("next_unit_price_quintile"),
        F.last("unit_price").over(w_sku_id_distinct_warehouse_ci).alias("last_unit_price"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "move_type",
        "unit_price",
        "unit_price_distance",
        "warehouse_capacity",
        "warehouse_city",
        "min_unit_price_so_far",
        "unit_price_quintile",
        "operator_sku_category_avg_unit_price_distance",
        "operator_sku_category_max_qty",
        "qty_vs_operator_sku_category_avg",
        "unit_price_avg_7_share_of_warehouse_city_unit_pr",
        "next_unit_price_quintile",
        "last_unit_price",
        F.greatest(F.col("warehouse_capacity"), F.col("unit_price_distance")).alias("greatest_warehouse_capacity_unit_price_distance"),
    )
)
