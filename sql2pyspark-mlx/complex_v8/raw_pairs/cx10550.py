from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy("qty")
w_operator = Window.partitionBy("operator")
w_warehouse_id_sku_category = Window.partitionBy("warehouse_id", "sku_category")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy("moved_at", "id")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_all_by_warehouse_id_sku_cate = Window.orderBy("warehouse_id_sku_category_avg_unit_price", "id")
w_all_by_warehouse_id_sku_cate_2 = Window.orderBy("warehouse_id_sku_category_avg_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_moved_at_month_sku_cate = Window.partitionBy(F.date_trunc("month", "prev_moved_at"), "sku_category_group").orderBy(F.col("moved_at").desc(), "id")
w_sku_category_by_distinct_sku = Window.partitionBy("sku_category").orderBy("distinct_sku_category_in_operator", "id")
w_qty_tercile = Window.partitionBy("qty_tercile")
w_distinct_sku_ids_by_n_sku_ca = Window.partitionBy("distinct_sku_ids").orderBy("n_sku_category_beauty", "moved_at_year", "qty_tercile").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_tercile_by_n_operator_no = Window.partitionBy("qty_tercile").orderBy("n_operator_not_in_op_eve_op_an", "moved_at_year", "qty_tercile").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_qty_tercile_by_n_operator_no_2 = Window.partitionBy("qty_tercile").orderBy("n_operator_not_in_op_eve_op_an", "moved_at_year", "qty_tercile")

prepared = (
    df_stock_moves
    .filter(~F.col("move_type").isin("OUTBOUND", "INBOUND"))
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
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.size(F.collect_set("sku_category").over(w_operator)).alias("distinct_sku_category_in_operator"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_sku_category), 2).alias("unit_price_vs_warehouse_id_sku_category_avg"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator), 2).alias("unit_price_vs_operator_avg"),
        F.max("qty").over(w_operator).alias("operator_max_qty"),
        F.lag("moved_at").over(w_sku_id_by_moved_at).alias("prev_moved_at"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id_sku_category), 2).alias("qty_vs_warehouse_id_sku_category_avg"),
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_category), 2).alias("warehouse_id_sku_category_avg_unit_price"),
        F.ntile(3).over(w_warehouse_id_by_qty).alias("qty_tercile"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id_sku_category), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id_sku_category"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.round(F.col("operator_max_qty") / F.nullif(F.col("qty_percent_rank"), F.lit(0)), 3).alias("operator_max_qty_per_qty_percent_rank"),
        F.when(F.col("sku_category").isin("Toys", "Beauty"), "group_a").when(F.col("sku_category") == "Food", "group_b").otherwise("other").alias("sku_category_group"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_warehouse_id_sku_cate), 3).alias("warehouse_id_sku_category_avg_unit_price_cume_di"),
        F.sum("qty_tercile").over(w_all_by_warehouse_id_sku_cate_2).alias("running_qty_tercile"),
        F.lead("qty_percent_rank").over(w_prev_moved_at_month_sku_cate).alias("next_qty_percent_rank"),
        F.rank().over(w_sku_category_by_distinct_sku).alias("distinct_sku_category_in_operator_rank"),
    )
)

summary = (
    sequenced
    .groupBy(F.year("moved_at").alias("moved_at_year"), "qty_tercile")
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.max("unit_price_vs_operator_avg").alias("max_unit_price_vs_operator_avg"),
        F.count(F.when(F.col("sku_category") != "Beauty", 1)).alias("n_sku_category_beauty"),
        F.count(F.when(~F.col("operator").isin("op_eve", "op_ann"), 1)).alias("n_operator_not_in_op_eve_op_an"),
    )
)

ranked = (
    summary
    .select(
        "moved_at_year",
        "qty_tercile",
        "distinct_sku_ids",
        "max_unit_price_vs_operator_avg",
        "n_sku_category_beauty",
        F.max("distinct_sku_ids").over(w_distinct_sku_ids_by_n_sku_ca).alias("max_distinct_sku_ids_so_far"),
        F.last("moved_at_year", ignorenulls=True).over(w_qty_tercile_by_n_operator_no).alias("last_known_moved_at_year"),
        F.lead("n_sku_category_beauty").over(w_qty_tercile_by_n_operator_no_2).alias("next_n_sku_category_beauty"),
    )
)

result = (
    ranked
    .select(
        "moved_at_year",
        "qty_tercile",
        "n_sku_category_beauty",
        "last_known_moved_at_year",
        (F.col("last_known_moved_at_year") + F.col("moved_at_year")).alias("last_known_moved_at_year_plus_moved_at_year"),
        F.round(F.col("distinct_sku_ids") / F.nullif(F.col("qty_tercile"), F.lit(0)), 3).alias("distinct_sku_ids_per_qty_tercile"),
    )
    .orderBy(F.col("moved_at_year").desc(), "qty_tercile")
    .limit(20)
)
