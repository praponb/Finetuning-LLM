from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_per_qty = Window.orderBy(F.col("unit_price_per_qty").desc_nulls_first(), "id")
w_all_by_unit_price_per_qty_ti = Window.orderBy(F.col("unit_price_per_qty").desc_nulls_first())

valid = (
    df_stock_moves
    .filter(F.col("sku_id") >= 8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.hour(F.col("st.moved_at")).between(7, 11)), "left_anti")
)

ranked = (
    qualified
    .select(
        "*",
        F.row_number().over(w_all_by_unit_price_per_qty).alias("seq_no"),
        F.rank().over(w_all_by_unit_price_per_qty_ti).alias("unit_price_per_qty_rank"),
        F.round(F.percent_rank().over(w_all_by_unit_price_per_qty_ti), 3).alias("unit_price_per_qty_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "unit_price_per_qty_rank",
        "qty",
        "seq_no",
    )
    .unpivot(
        ids=["id"],
        values=["unit_price_per_qty_rank", "qty", "seq_no"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
