from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_wareh = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("warehouse_opened_on").desc(), "id")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_sku_id_warehouse_city = Window.partitionBy("sku_id", "warehouse_city")
w_warehouse_city = Window.partitionBy("warehouse_city")
w_sku_id_warehouse_id_by_wareh_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_qty_prior_1 = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(-1, -1)
w_warehouse_city_operator_by_m = Window.partitionBy("warehouse_city", "operator").orderBy("moved_at", "id")
w_sku_id_by_qty_prior_3 = Window.partitionBy("sku_id").orderBy("qty", "id").rowsBetween(-3, -1)

base_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("INBOUND", "TRANSFER")) & (F.col("warehouse_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

combined_details = (
    base_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_operator_label",
        "s.operator_move_type_label",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        "sk.sku_code",
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_sku_id_warehouse_id_by_wareh)).alias("days_since_prev_warehouse_opened_on"),
        F.row_number().over(w_warehouse_id_by_moved_at).alias("rn"),
        F.lead("qty", 1, 0).over(w_warehouse_id_by_moved_at).alias("next_qty"),
        F.min("unit_price").over(w_sku_id_warehouse_city).alias("sku_id_warehouse_city_min_unit_price"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_city), 2).alias("qty_vs_warehouse_city_avg"),
        F.max("unit_price").over(w_sku_id_warehouse_city).alias("sku_id_warehouse_city_max_unit_price"),
        F.lag("unit_price").over(w_sku_id_warehouse_id_by_wareh).alias("prev_unit_price"),
        F.sum("qty").over(w_sku_id_warehouse_id_by_wareh_2).alias("running_qty"),
        F.round(F.avg("unit_price").over(w_sku_id_by_qty_prior_1), 2).alias("unit_price_avg_1_prior"),
        F.dense_rank().over(w_warehouse_city_operator_by_m).alias("moved_at_dense_rank"),
        F.round(F.avg("qty").over(w_sku_id_by_qty_prior_3), 2).alias("qty_avg_3_prior"),
    )
)

top_rows = sequenced.filter(F.col("rn") <= 5)

result = (
    top_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "move_type_operator_label",
        "operator_move_type_label",
        "warehouse_opened_on",
        "sku_code",
        "days_since_prev_warehouse_opened_on",
        "rn",
        "next_qty",
        "sku_id_warehouse_city_min_unit_price",
        "qty_vs_warehouse_city_avg",
        "sku_id_warehouse_city_max_unit_price",
        "prev_unit_price",
        "running_qty",
        "moved_at_dense_rank",
        "qty_avg_3_prior",
        F.date_format(F.col("warehouse_opened_on"), "yyyy-MM-dd").alias("warehouse_opened_on_label"),
    )
    .orderBy("sku_id_warehouse_city_min_unit_price", "id")
    .limit(20)
)
