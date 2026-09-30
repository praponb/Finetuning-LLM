from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_qty = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_sku_id_sku_category_by_wareh = Window.partitionBy("sku_id", "sku_category").orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_category_operator = Window.partitionBy("sku_category", "operator")
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_sku_id_sku_category_by_wareh_2 = Window.partitionBy("sku_id", "sku_category").orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_operator_by_war = Window.partitionBy("warehouse_id", "operator").orderBy("warehouse_capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_qty_prior_3 = Window.partitionBy("operator").orderBy(F.col("qty").desc(), "id").rowsBetween(-3, -1)

recent = df_stock_moves.filter((F.col("warehouse_id") <= 2) & (F.col("unit_price") >= 205.1))

combined_details_warehouse = (
    recent.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.lower(F.col("w.wh_code")).like("%x1%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

ranked = (
    combined_details_warehouse
    .select(
        "*",
        F.dense_rank().over(w_operator_by_qty).alias("qty_dense_rank"),
        F.max("warehouse_capacity").over(w_sku_id_operator).alias("sku_id_operator_max_warehouse_capacity"),
        F.nth_value("warehouse_capacity", 2).over(w_sku_id_sku_category_by_wareh).alias("second_warehouse_capacity"),
        F.round(F.col("warehouse_capacity") - F.avg("warehouse_capacity").over(w_sku_category_operator), 2).alias("warehouse_capacity_vs_sku_category_operator_avg"),
        F.round(F.percent_rank().over(w_warehouse_id_by_unit_price), 3).alias("unit_price_percent_rank"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id_operator), 2).alias("qty_vs_sku_id_operator_avg"),
        F.lead("warehouse_capacity").over(w_operator_by_qty).alias("next_warehouse_capacity"),
        F.first("unit_price", ignorenulls=True).over(w_sku_id_sku_category_by_wareh_2).alias("first_known_unit_price"),
        F.last("warehouse_capacity", ignorenulls=True).over(w_warehouse_id_operator_by_war).alias("last_known_warehouse_capacity"),
        F.round(F.avg("sku_unit_cost").over(w_operator_by_qty_prior_3), 2).alias("sku_unit_cost_avg_3_prior"),
        F.round(F.col("sku_unit_cost") - F.avg("sku_unit_cost").over(w_sku_category_operator), 2).alias("sku_unit_cost_vs_sku_category_operator_avg"),
        F.count(F.lit(1)).over(w_sku_category_operator).alias("rows_in_sku_category_operator"),
    )
)

result = (
    ranked
    .filter(F.col("sku_id_operator_max_warehouse_capacity") >= 40800)
    .select(
        "id",
        "sku_id",
        "qty",
        "unit_price",
        "warehouse_capacity",
        "sku_category",
        "qty_dense_rank",
        "sku_id_operator_max_warehouse_capacity",
        "warehouse_capacity_vs_sku_category_operator_avg",
        "unit_price_percent_rank",
        "next_warehouse_capacity",
        "last_known_warehouse_capacity",
        "sku_unit_cost_avg_3_prior",
        "sku_unit_cost_vs_sku_category_operator_avg",
        "rows_in_sku_category_operator",
        F.round(F.col("unit_price") / F.nullif(F.col("next_warehouse_capacity"), F.lit(0)), 3).alias("unit_price_per_next_warehouse_capacity"),
        (F.floor(F.col("qty_dense_rank") / 2) * 2).alias("qty_dense_rank_bucket"),
    )
    .orderBy(
        "sku_unit_cost_avg_3_prior",
        F.col("sku_id_operator_max_warehouse_capacity").desc_nulls_first(),
    )
)
