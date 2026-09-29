from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_category = Window.partitionBy("warehouse_id", "sku_category")
w_warehouse_id_by_moved_at_plu = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at_plus_30m").asc_nulls_last(), "id")
w_sku_category_by_unit_price_p = Window.partitionBy("sku_category").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-6, -1)
w_sku_category_operator_by_qty = Window.partitionBy("sku_category", "operator").orderBy("qty")
w_moved_at_month_by_unit_price = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_city = Window.partitionBy("warehouse_city")
w_warehouse_id_by_moved_at_plu_2 = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at_plus_30m").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_plus_30m_month = Window.partitionBy(F.month("moved_at_plus_30m"))
w_next3_moved_at_plus_30m_year = Window.partitionBy(F.year("next3_moved_at_plus_30m")).orderBy(F.col("running_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next3_moved_at_plus_30m_year_2 = Window.partitionBy(F.year("next3_moved_at_plus_30m")).orderBy(F.col("running_count").desc(), "id")
w_all_by_warehouse_city_avg_qt = Window.orderBy("warehouse_city_avg_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .filter(F.col("qty") < 387)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.*",
        F.col("sk.category").alias("sku_category"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "qty",
        "operator",
        "unit_price_adj",
        "moved_at_plus_30m",
        "sku_category",
        "warehouse_wh_code",
        "warehouse_city",
        F.sum("unit_price").over(w_warehouse_id_sku_category).alias("warehouse_id_sku_category_sum_unit_price"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at_plu), 3).alias("moved_at_plus_30m_percent_rank"),
        F.round(F.avg("unit_price").over(w_sku_category_by_unit_price_p), 2).alias("unit_price_avg_6_prior"),
        F.round(F.percent_rank().over(w_sku_category_operator_by_qty), 3).alias("qty_percent_rank"),
        F.count(F.lit(1)).over(w_moved_at_month_by_unit_price).alias("running_count"),
        F.round(F.avg("qty").over(w_warehouse_city), 2).alias("warehouse_city_avg_qty"),
        F.first("unit_price", ignorenulls=True).over(w_warehouse_id_by_moved_at_plu_2).alias("first_known_unit_price"),
        F.lead("moved_at_plus_30m", 3).over(w_warehouse_id_by_moved_at_plu).alias("next3_moved_at_plus_30m"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.round(F.avg("warehouse_city_avg_qty").over(w_moved_at_plus_30m_month), 2).alias("moved_at_plus_30m_month_avg_warehouse_city_avg_q"),
        F.min("unit_price_avg_6_prior").over(w_next3_moved_at_plus_30m_year).alias("min_unit_price_avg_6_prior_so_far"),
        F.round(F.col("first_known_unit_price") / F.nullif(F.sum("first_known_unit_price").over(w_moved_at_plus_30m_month), F.lit(0)), 4).alias("first_known_unit_price_share_of_moved_at_plus_30"),
        F.count(F.lit(1)).over(w_moved_at_plus_30m_month).alias("rows_in_moved_at_plus_30m_month"),
        F.size(F.collect_set("warehouse_id").over(w_moved_at_plus_30m_month)).alias("distinct_warehouse_id_in_moved_at_plus_30m_month"),
        F.round(F.cume_dist().over(w_next3_moved_at_plus_30m_year_2), 3).alias("running_count_cume_dist"),
        F.min("running_count").over(w_all_by_warehouse_city_avg_qt).alias("min_running_count_so_far"),
    )
)

result = (
    windowed
    .filter(F.col("qty_percent_rank") <= 0.6)
    .select(
        "id",
        "warehouse_id",
        "qty",
        "operator",
        "unit_price_adj",
        "moved_at_plus_30m",
        "sku_category",
        "warehouse_wh_code",
        "warehouse_city",
        "warehouse_id_sku_category_sum_unit_price",
        "moved_at_plus_30m_percent_rank",
        "qty_percent_rank",
        "running_count",
        "first_known_unit_price",
        "next3_moved_at_plus_30m",
        "moved_at_plus_30m_month_avg_warehouse_city_avg_q",
        "min_unit_price_avg_6_prior_so_far",
        "first_known_unit_price_share_of_moved_at_plus_30",
        "distinct_warehouse_id_in_moved_at_plus_30m_month",
        "running_count_cume_dist",
        "min_running_count_so_far",
        F.when(F.col("unit_price_adj") >= 487.9, "HIGH").when(F.col("unit_price_adj") >= 338.1, "MEDIUM").otherwise("LOW").alias("unit_price_adj_tier"),
        F.date_format(F.col("moved_at_plus_30m"), "MMM yyyy").alias("moved_at_plus_30m_label"),
    )
    .orderBy(F.col("next3_moved_at_plus_30m").desc(), "min_running_count_so_far")
)
