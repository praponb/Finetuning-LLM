from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date_operator_by = Window.partitionBy(F.to_date("moved_at"), "operator").orderBy("unit_price", "id").rowsBetween(-1, -1)
w_move_type = Window.partitionBy("move_type")
w_moved_at_to_date_operator_by_2 = Window.partitionBy(F.to_date("moved_at"), "operator").orderBy("unit_price", "id").rowsBetween(-2, -1)
w_moved_at_to_date_by_qty_ties = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("qty").desc())
w_moved_at_to_date_operator_by_3 = Window.partitionBy(F.to_date("moved_at"), "operator").orderBy("unit_price", "id")
w_moved_at_to_date_by_qty = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("qty").desc(), "id")

base = (
    df_stock_moves
    .filter((F.col("operator").isin("op_dew", "op_eve")) | (F.col("sku_id") >= 11))
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

qualified = (
    base
    .crossJoin(base.agg(F.max("unit_price").alias("overall_max_unit_price")))
    .filter(F.col("unit_price") < F.col("overall_max_unit_price"))
    .drop("overall_max_unit_price")
)

sequenced = (
    qualified
    .select(
        "*",
        F.sum("qty").over(w_moved_at_to_date_operator_by).alias("qty_sum_1_prior"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.sum("qty").over(w_moved_at_to_date_operator_by_2).alias("qty_sum_2_prior"),
        F.round(F.cume_dist().over(w_moved_at_to_date_by_qty_ties), 3).alias("qty_cume_dist"),
        F.round(F.percent_rank().over(w_moved_at_to_date_operator_by_3), 3).alias("unit_price_percent_rank"),
        F.dense_rank().over(w_moved_at_to_date_by_qty).alias("qty_dense_rank"),
    )
)

without_matches = (
    sequenced.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 17), "left_semi")
    .filter(F.col("qty_cume_dist") < 0.7)
)

result = (
    without_matches
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "rows_in_move_type",
        "qty_sum_2_prior",
        "unit_price_percent_rank",
        "qty_dense_rank",
    )
    .orderBy("sku_id", F.col("moved_at").desc())
)
