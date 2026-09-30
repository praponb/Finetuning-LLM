from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_is_active_warehouse_open = Window.partitionBy("sku_is_active", F.year("warehouse_opened_on")).orderBy("warehouse_capacity", "id")
w_moved_at_year_by_warehouse_c = Window.partitionBy(F.year("moved_at")).orderBy("warehouse_capacity", "id").rowsBetween(-3, -1)
w_operator = Window.partitionBy("operator")
w_sku_is_active_warehouse_open_2 = Window.partitionBy("sku_is_active", F.year("warehouse_opened_on")).orderBy("warehouse_capacity", "id").rowsBetween(-2, -1)
w_operator_warehouse_id_by_war = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("warehouse_capacity").desc(), "id")
w_sku_is_active_warehouse_open_3 = Window.partitionBy("sku_is_active", F.year("warehouse_opened_on")).orderBy("warehouse_capacity", "id").rowsBetween(-1, -1)
w_sku_id = Window.partitionBy("sku_id")
w_seq_no_by_prev_moved_at_hour = Window.partitionBy("seq_no").orderBy(F.col("prev_moved_at_hour").asc_nulls_last(), "id")
w_seq_no_by_warehouse_opened_o = Window.partitionBy("seq_no").orderBy(F.col("warehouse_opened_on").asc_nulls_last())
w_seq_no_by_warehouse_opened_o_2 = Window.partitionBy("seq_no").orderBy(F.col("warehouse_opened_on").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_seq_no_by_prev_moved_at_hour_2 = Window.partitionBy("seq_no").orderBy(F.col("prev_moved_at_hour").asc_nulls_last())

cleaned = (
    df_stock_moves
    .filter((F.col("qty") > 116) & (F.col("unit_price").between(95.8, 393.3)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category").isin("Beauty", "Apparel", "Electronics")), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(~F.col("sk.is_active"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.moved_at_hour",
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "sku_id",
        "operator",
        "warehouse_opened_on",
        F.row_number().over(w_sku_is_active_warehouse_open).alias("seq_no"),
        F.max("qty").over(w_moved_at_year_by_warehouse_c).alias("qty_max_3_prior"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator), F.lit(0)), 4).alias("qty_share_of_operator"),
        F.dense_rank().over(w_sku_is_active_warehouse_open).alias("warehouse_capacity_dense_rank"),
        F.round(F.avg("qty").over(w_sku_is_active_warehouse_open_2), 2).alias("qty_avg_2_prior"),
        F.round(F.avg("warehouse_capacity").over(w_operator), 2).alias("operator_avg_warehouse_capacity"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.lag("unit_price").over(w_operator_warehouse_id_by_war).alias("prev_unit_price"),
        F.max("qty").over(w_operator).alias("operator_max_qty"),
        F.lag("moved_at_hour").over(w_sku_is_active_warehouse_open).alias("prev_moved_at_hour"),
        F.ntile(3).over(w_sku_is_active_warehouse_open).alias("warehouse_capacity_tercile"),
        F.round(F.avg("unit_price").over(w_sku_is_active_warehouse_open_3), 2).alias("unit_price_avg_1_prior"),
    )
)

top_rows = (
    ranked
    .filter((F.col("warehouse_capacity_dense_rank") <= 3) & (F.col("operator_avg_warehouse_capacity") < 16200.0))
    .drop("warehouse_capacity_dense_rank")
)

metrics = (
    top_rows
    .select(
        "id",
        "sku_id",
        "warehouse_opened_on",
        "seq_no",
        "qty_share_of_operator",
        "qty_avg_2_prior",
        "operator_avg_warehouse_capacity",
        "prev_unit_price",
        "operator_max_qty",
        "prev_moved_at_hour",
        "warehouse_capacity_tercile",
        "unit_price_avg_1_prior",
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.lead("prev_unit_price", 2, 0).over(w_seq_no_by_prev_moved_at_hour).alias("next2_prev_unit_price"),
        F.round(F.cume_dist().over(w_seq_no_by_warehouse_opened_o), 3).alias("warehouse_opened_on_cume_dist"),
        F.round(F.col("qty_max_3_prior") / F.nullif(F.sum("qty_max_3_prior").over(w_sku_id), F.lit(0)), 4).alias("qty_max_3_prior_share_of_sku_id"),
        F.min("warehouse_capacity_tercile").over(w_sku_id).alias("sku_id_min_warehouse_capacity_tercile"),
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
        F.max("prev_unit_price").over(w_seq_no_by_warehouse_opened_o_2).alias("prev_unit_price_max_2_prior"),
        F.round(F.percent_rank().over(w_seq_no_by_prev_moved_at_hour_2), 3).alias("prev_moved_at_hour_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_opened_on",
        "seq_no",
        "qty_share_of_operator",
        "qty_avg_2_prior",
        "operator_avg_warehouse_capacity",
        "prev_moved_at_hour",
        "warehouse_capacity_tercile",
        "unit_price_avg_1_prior",
        "rows_in_sku_id",
        "next2_prev_unit_price",
        "warehouse_opened_on_cume_dist",
        "qty_max_3_prior_share_of_sku_id",
        "sku_id_min_warehouse_capacity_tercile",
        "prev_moved_at_hour_percent_rank",
    )
)
