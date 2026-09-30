from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_qty_prior_1 = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(-1, -1)
w_warehouse_id_by_qty = Window.partitionBy("warehouse_id").orderBy("qty", "id")
w_warehouse_id_by_qty_running = Window.partitionBy("warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

windowed = (
    cleaned_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.round(F.avg("unit_price").over(w_warehouse_id_by_qty_prior_1), 2).alias("unit_price_avg_1_prior"),
        F.lead("unit_price", 3).over(w_warehouse_id_by_qty).alias("next3_unit_price"),
        F.sum("qty").over(w_warehouse_id_by_qty_running).alias("running_qty"),
    )
)

per_group = (
    windowed
    .filter(F.col("qty") > 67)
    .groupBy("sku_id", "operator", "move_type")
    .agg(
        F.max("next3_unit_price").alias("max_next3_unit_price"),
        F.count(F.when(~F.col("move_type").isin("ADJUST", "INBOUND"), 1)).alias("n_move_type_not_in_adjust_inbo"),
        F.round(100 * F.count(F.when(F.col("operator").isin("op_dew", "op_eve"), 1)) / F.count("*"), 1).alias("pct_operator_in_op_dew_op_eve"),
    )
)

result = (
    per_group
    .select(
        "sku_id",
        "operator",
        "pct_operator_in_op_dew_op_eve",
        "max_next3_unit_price",
    )
    .unpivot(
        ids=["sku_id", "operator"],
        values=["pct_operator_in_op_dew_op_eve", "max_next3_unit_price"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("sku_id", "operator", "field")
)
