from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_sku_id_operator_by_sku_weigh = Window.partitionBy("sku_id", "operator").orderBy("sku_weight_kg", "id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_sku_category = Window.partitionBy("sku_category")
w_warehouse_city_by_sku_weight = Window.partitionBy("warehouse_city").orderBy(F.col("sku_weight_kg").asc_nulls_last(), "id")
w_all_by_unit_price_ties = Window.orderBy(F.col("unit_price").desc())
w_operator_sku_category_by_mov = Window.partitionBy("operator", "sku_category").orderBy("moved_at", "id")

cleaned = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("warehouse_id") <= 5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

enriched_sku = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_slug",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.city").alias("warehouse_city"),
    )
)

windowed = (
    enriched_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        "sku_category",
        "sku_weight_kg",
        "warehouse_city",
        F.sum("sku_weight_kg").over(w_sku_id).alias("sku_id_sum_sku_weight_kg"),
        F.ntile(4).over(w_sku_id_operator_by_sku_weigh).alias("sku_weight_kg_quartile"),
        F.lag("moved_at").over(w_all_by_unit_price).alias("prev_moved_at"),
        F.count(F.lit(1)).over(w_sku_category).alias("rows_in_sku_category"),
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
        F.round(F.percent_rank().over(w_warehouse_city_by_sku_weight), 3).alias("sku_weight_kg_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        F.size(F.collect_set("warehouse_city").over(w_sku_category)).alias("distinct_warehouse_city_in_sku_category"),
        F.rank().over(w_operator_sku_category_by_mov).alias("moved_at_rank"),
    )
)

first_rows = windowed.filter(F.col("moved_at_rank") <= 3)

result = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        "sku_category",
        "sku_weight_kg",
        "sku_id_sum_sku_weight_kg",
        "prev_moved_at",
        "rows_in_sku_category",
        "sku_weight_kg_percent_rank",
        "unit_price_cume_dist",
        "distinct_warehouse_city_in_sku_category",
        "moved_at_rank",
        (F.floor(F.col("moved_at_rank") / 2) * 2).alias("moved_at_rank_bucket"),
    )
    .orderBy("sku_category")
)
