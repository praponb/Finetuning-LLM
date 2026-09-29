from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_operator_by_m = Window.partitionBy(F.date_trunc("month", "moved_at"), "operator").orderBy("moved_at", "id")
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")

prepared = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(8, 14)) & (~F.col("move_type").isin("TRANSFER", "OUTBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.rank().over(w_moved_at_month_operator_by_m).alias("moved_at_rank"),
        F.row_number().over(w_move_type_by_qty).alias("seq_no"),
        F.round(F.col("qty_filled") / F.nullif(F.sum("qty_filled").over(w_warehouse_id), F.lit(0)), 4).alias("qty_filled_share_of_warehouse_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        F.col("unit_price").alias("m_unit_price"),
        F.col("qty_filled_share_of_warehouse_id").alias("qty_filled_share_of_warehouse_id_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_unit_price", "qty_filled_share_of_warehouse_id_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
