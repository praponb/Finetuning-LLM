from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator = Window.partitionBy("move_type", "operator")
w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy("unit_price", "id")
w_warehouse_id_by_unit_price_f = Window.partitionBy("warehouse_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_operator_by_sku_unit = Window.partitionBy("sku_id", "operator").orderBy("sku_unit_cost", "id")
w_sku_id_move_type = Window.partitionBy("sku_id", "move_type")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id")
w_move_type_by_qty_ties = Window.partitionBy("move_type").orderBy(F.col("qty").desc())

base_moves = df_stock_moves.filter(F.hour(F.col("moved_at")).between(8, 13))

with_details = (
    base_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.upper(F.trim(F.col("w.wh_code"))).like("K%")), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.capacity") <= 11506)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "warehouse_capacity",
        "sku_unit_cost",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type_operator), 2).alias("unit_price_vs_move_type_operator_avg"),
        F.row_number().over(w_warehouse_id_by_unit_price).alias("rn"),
        F.nth_value("sku_unit_cost", 2).over(w_warehouse_id_by_unit_price_f).alias("second_sku_unit_cost"),
        F.row_number().over(w_sku_id_operator_by_sku_unit).alias("rn_2"),
        F.dense_rank().over(w_sku_id_operator_by_sku_unit).alias("sku_unit_cost_dense_rank"),
        F.max("qty").over(w_sku_id_move_type).alias("sku_id_move_type_max_qty"),
        F.lead("sku_unit_cost").over(w_move_type_by_qty).alias("next_sku_unit_cost"),
        F.rank().over(w_move_type_by_qty_ties).alias("qty_rank"),
        (F.col("sku_unit_cost") - F.lag("sku_unit_cost").over(w_sku_id_operator_by_sku_unit)).alias("sku_unit_cost_change"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("warehouse_capacity"), F.lit(0)).alias("warehouse_capacity_filled"),
        F.coalesce(F.col("sku_unit_cost_change"), F.lit(0.0)).alias("sku_unit_cost_change_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

result = (
    scored
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "warehouse_capacity",
        "sku_unit_cost",
        "unit_price_vs_move_type_operator_avg",
        "second_sku_unit_cost",
        "rn_2",
        "sku_unit_cost_dense_rank",
        "sku_id_move_type_max_qty",
        "next_sku_unit_cost",
        "sku_unit_cost_change",
        "warehouse_capacity_filled",
        "sku_unit_cost_change_filled",
        "operator_slug",
        F.when(F.col("move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_type") == "INBOUND", "group_b").otherwise("other").alias("move_type_group"),
    )
)
