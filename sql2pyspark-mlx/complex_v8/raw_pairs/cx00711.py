from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator = Window.partitionBy("move_type", "operator")
w_warehouse_id_sku_category = Window.partitionBy("warehouse_id", "sku_category")
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy(F.col("qty").asc_nulls_last(), "id")
w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy("unit_price", "id").rowsBetween(-6, -1)
w_warehouse_id_by_qty_full = Window.partitionBy("warehouse_id").orderBy(F.col("qty").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_qty = Window.orderBy("qty", "id")
w_sku_category_sku_category_gr = Window.partitionBy("sku_category", "sku_category_group")
w_all_by_prev_unit_price_minus = Window.orderBy(F.col("prev_unit_price_minus_qty_quintile").asc_nulls_last(), "id")
w_sku_category_rn_by_distinct = Window.partitionBy("sku_category", "rn").orderBy(F.col("distinct_warehouse_id_in_move_type_operator").asc_nulls_last(), "id")

base_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_dew") | (F.month(F.col("moved_at")).isin(2, 3)))
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
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "sk.sku_code",
        F.col("sk.category").alias("sku_category"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "sku_category",
        F.size(F.collect_set("warehouse_id").over(w_move_type_operator)).alias("distinct_warehouse_id_in_move_type_operator"),
        F.max("qty").over(w_warehouse_id_sku_category).alias("warehouse_id_sku_category_max_qty"),
        F.ntile(5).over(w_warehouse_id_by_qty).alias("qty_quintile"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_sku_category), 2).alias("unit_price_vs_warehouse_id_sku_category_avg"),
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_id_by_unit), 2).alias("unit_price_avg_6_prior"),
        F.first("unit_price").over(w_warehouse_id_by_qty_full).alias("first_unit_price"),
        F.row_number().over(w_all_by_qty).alias("rn"),
        F.lag("unit_price").over(w_all_by_qty).alias("prev_unit_price"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.when(F.col("sku_category").isin("Beauty", "Food"), "group_a").when(F.col("sku_category") == "Electronics", "group_b").otherwise("other").alias("sku_category_group"),
        (F.col("prev_unit_price") - F.col("qty_quintile")).alias("prev_unit_price_minus_qty_quintile"),
        F.substring(F.trim(F.col("sku_category")), 1, 2).alias("sku_category_prefix"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "sku_category",
        "warehouse_id_sku_category_max_qty",
        "unit_price_vs_warehouse_id_sku_category_avg",
        "unit_price_avg_6_prior",
        "first_unit_price",
        "rn",
        "prev_unit_price",
        "prev_unit_price_minus_qty_quintile",
        F.max("first_unit_price").over(w_sku_category_sku_category_gr).alias("sku_category_sku_category_group_max_first_unit_p"),
        F.round(F.avg("unit_price").over(w_sku_category_sku_category_gr), 2).alias("sku_category_sku_category_group_avg_unit_price"),
        F.row_number().over(w_all_by_prev_unit_price_minus).alias("rn_2"),
        F.dense_rank().over(w_all_by_prev_unit_price_minus).alias("prev_unit_price_minus_qty_quintile_dense_rank"),
        F.ntile(10).over(w_sku_category_rn_by_distinct).alias("distinct_warehouse_id_in_move_type_operator_deci"),
        F.ntile(3).over(w_all_by_prev_unit_price_minus).alias("prev_unit_price_minus_qty_quintile_tercile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "sku_id",
        "qty",
        "sku_category",
        "warehouse_id_sku_category_max_qty",
        "unit_price_vs_warehouse_id_sku_category_avg",
        "unit_price_avg_6_prior",
        "first_unit_price",
        "rn",
        "prev_unit_price",
        "prev_unit_price_minus_qty_quintile",
        "sku_category_sku_category_group_max_first_unit_p",
        "sku_category_sku_category_group_avg_unit_price",
        "rn_2",
        "prev_unit_price_minus_qty_quintile_dense_rank",
        "distinct_warehouse_id_in_move_type_operator_deci",
        "prev_unit_price_minus_qty_quintile_tercile",
        F.lower(F.regexp_replace(F.trim(F.col("sku_category")), " ", "_")).alias("sku_category_slug"),
        (F.col("qty") + F.col("unit_price_avg_6_prior")).alias("qty_plus_unit_price_avg_6_prior"),
    )
    .orderBy("rn", F.col("unit_price_avg_6_prior").desc(), "id")
    .limit(5)
)
