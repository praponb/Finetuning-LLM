from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-2, -1)
w_sku_category_sku_id = Window.partitionBy("sku_category", "sku_id")
w_all_by_unit_price_filled_tie = Window.orderBy(F.col("unit_price_filled").desc())
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_unit_price_tier_sku_id_by_qt = Window.partitionBy("unit_price_tier", "sku_id").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_sku_id_by_moved_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id_sku_id_by_moved_4 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_unit_price_tier_by_sku_categ = Window.partitionBy("unit_price_tier").orderBy(F.col("sku_category_sku_id_min_unit_price").desc(), "id")

filtered_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 290.4, "large").when(F.col("unit_price") >= 160.8, "medium").otherwise("small").alias("unit_price_tier"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

enriched_sku = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.unit_price_tier",
        "s.unit_price_filled",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

sequenced = (
    enriched_sku
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price_tier",
        "sku_labels",
        "sku_category",
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_id_by_moved), 2).alias("unit_price_avg_2_prior"),
        F.round(F.col("unit_price_filled") / F.nullif(F.sum("unit_price_filled").over(w_sku_category_sku_id), F.lit(0)), 4).alias("unit_price_filled_share_of_sku_category_sku_id"),
        F.round(F.cume_dist().over(w_all_by_unit_price_filled_tie), 3).alias("unit_price_filled_cume_dist"),
        F.first("unit_price", ignorenulls=True).over(w_warehouse_id_sku_id_by_moved_2).alias("first_known_unit_price"),
        F.sum("unit_price_filled").over(w_unit_price_tier_sku_id_by_qt).alias("running_unit_price_filled"),
        F.lead("unit_price_filled").over(w_warehouse_id_sku_id_by_moved_3).alias("next_unit_price_filled"),
        F.nth_value("qty", 2).over(w_warehouse_id_sku_id_by_moved_4).alias("second_qty"),
        F.size(F.collect_set("warehouse_id").over(w_sku_category_sku_id)).alias("distinct_warehouse_id_in_sku_category_sku_id"),
        F.min("unit_price").over(w_sku_category_sku_id).alias("sku_category_sku_id_min_unit_price"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "operator",
        "sku_labels",
        "unit_price_filled_share_of_sku_category_sku_id",
        "next_unit_price_filled",
        "second_qty",
        "distinct_warehouse_id_in_sku_category_sku_id",
        "sku_category_sku_id_min_unit_price",
        F.round(F.col("next_unit_price_filled") - F.avg("next_unit_price_filled").over(w_sku_category_sku_id), 2).alias("next_unit_price_filled_vs_sku_category_sku_id_av"),
        F.count(F.lit(1)).over(w_sku_category_sku_id).alias("rows_in_sku_category_sku_id"),
        (F.col("next_unit_price_filled") - F.lag("next_unit_price_filled").over(w_unit_price_tier_by_sku_categ)).alias("next_unit_price_filled_change"),
        F.size(F.collect_set("unit_price_tier").over(w_sku_category_sku_id)).alias("distinct_unit_price_tier_in_sku_category_sku_id"),
        F.lead("next_unit_price_filled").over(w_unit_price_tier_by_sku_categ).alias("next_next_unit_price_filled"),
    )
)

result = (
    metrics
    .filter(F.col("distinct_unit_price_tier_in_sku_category_sku_id") > 5)
    .select(
        "id",
        "operator",
        "sku_labels",
        "next_unit_price_filled",
        "second_qty",
        "distinct_warehouse_id_in_sku_category_sku_id",
        "sku_category_sku_id_min_unit_price",
        "next_unit_price_filled_change",
        "distinct_unit_price_tier_in_sku_category_sku_id",
        "next_next_unit_price_filled",
        F.when(F.col("second_qty") >= 357, "large").when(F.col("second_qty") >= 123, "medium").otherwise("small").alias("second_qty_tier"),
        F.size(F.col("sku_labels")).alias("sku_labels_count"),
    )
    .orderBy(
        F.col("sku_category_sku_id_min_unit_price").desc_nulls_first(),
        F.col("id").desc_nulls_first(),
    )
)
