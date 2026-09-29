from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_prior_3 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-3, -1)
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")

valid_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date")) | (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("operator").isin("op_bee", "op_dew"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

with_windows = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "operator_group",
        "least_qty_unit_price",
        F.sum("qty").over(w_all_by_qty_prior_3).alias("qty_sum_3_prior"),
        F.lead("qty").over(w_all_by_qty).alias("next_qty"),
        F.round(F.col("least_qty_unit_price") / F.nullif(F.sum("least_qty_unit_price").over(w_operator_warehouse_id), F.lit(0)), 4).alias("least_qty_unit_price_share_of_operator_warehouse"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "least_qty_unit_price",
        "least_qty_unit_price_share_of_operator_warehouse",
    )
    .unpivot(
        ids=["id"],
        values=["least_qty_unit_price", "least_qty_unit_price_share_of_operator_warehouse"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
