from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_prior_6 = Window.orderBy("moved_at", "id").rowsBetween(-6, -1)
w_sku_id_warehouse_id_by_moved = Window.partitionBy("sku_id", "warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_sku_id_by_qty_p = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty_plus_unit_price").desc(), "id").rowsBetween(-1, -1)
w_operator_sku_id_by_qty = Window.partitionBy("operator", "sku_id").orderBy("qty", "id")
w_warehouse_id_sku_id_by_qty_p_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty_plus_unit_price").desc(), "id").rowsBetween(-6, -1)
w_warehouse_id_sku_id_by_qty_p_3 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty_plus_unit_price").desc(), "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_operator = Window.partitionBy("operator")
w_sku_id_operator_by_unit_pric = Window.partitionBy("sku_id", "operator").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_sku_id_by_qty_p_4 = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty_plus_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_operator_by_unit_pric_2 = Window.partitionBy("sku_id", "operator").orderBy(F.col("unit_price").desc())
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")

base_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

metrics = (
    base_moves
    .select(
        "id",
        "sku_id",
        "moved_at",
        "operator",
        "qty_plus_unit_price",
        F.sum("qty_plus_unit_price").over(w_all_by_moved_at_prior_6).alias("qty_plus_unit_price_sum_6_prior"),
        F.last("unit_price").over(w_sku_id_warehouse_id_by_moved).alias("last_unit_price"),
        F.round(F.avg("unit_price").over(w_warehouse_id_sku_id_by_qty_p), 2).alias("unit_price_avg_1_prior"),
        F.lead("moved_at", 2).over(w_operator_sku_id_by_qty).alias("next2_moved_at"),
        F.max("unit_price").over(w_warehouse_id_sku_id_by_qty_p_2).alias("unit_price_max_6_prior"),
        F.lead("qty_plus_unit_price").over(w_warehouse_id_sku_id_by_qty_p_3).alias("next_qty_plus_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_operator), F.lit(0)), 4).alias("unit_price_share_of_sku_id_operator"),
        F.size(F.collect_set("warehouse_id").over(w_operator)).alias("distinct_warehouse_id_in_operator"),
        F.ntile(5).over(w_sku_id_operator_by_unit_pric).alias("unit_price_quintile"),
        F.nth_value("moved_at", 2).over(w_warehouse_id_sku_id_by_qty_p_4).alias("second_moved_at"),
        F.lead("qty_plus_unit_price").over(w_sku_id_operator_by_unit_pric).alias("next_qty_plus_unit_price_2"),
        F.sum("qty_plus_unit_price").over(w_operator).alias("operator_sum_qty_plus_unit_price"),
        F.dense_rank().over(w_sku_id_operator_by_unit_pric_2).alias("unit_price_dense_rank"),
        F.round(F.col("qty_plus_unit_price") - F.avg("qty_plus_unit_price").over(w_sku_id_warehouse_id), 2).alias("qty_plus_unit_price_vs_sku_id_warehouse_id_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "sku_id",
        "moved_at",
        "qty_plus_unit_price",
        "unit_price_avg_1_prior",
        "next_qty_plus_unit_price",
        "distinct_warehouse_id_in_operator",
        "unit_price_quintile",
        "second_moved_at",
        "next_qty_plus_unit_price_2",
        "operator_sum_qty_plus_unit_price",
        "unit_price_dense_rank",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.when(F.col("last_unit_price") >= 313.8, "A").when(F.col("last_unit_price") >= 160.8, "B").otherwise("C").alias("last_unit_price_tier"),
    )
    .orderBy(F.col("qty_plus_unit_price").desc())
)
