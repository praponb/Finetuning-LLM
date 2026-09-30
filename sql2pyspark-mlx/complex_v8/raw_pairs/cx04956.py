from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_operator_sku_id_by_unit_pric = Window.partitionBy("operator", "sku_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, Window.currentRow)
w_latest = Window.partitionBy("operator").orderBy("unit_price_avg_2", "id")

base_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

windowed = (
    base_moves
    .select(
        "*",
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.size(F.collect_set("sku_id").over(w_move_type)).alias("distinct_sku_id_in_move_type"),
        F.round(F.avg("unit_price").over(w_operator_sku_id_by_unit_pric), 2).alias("unit_price_avg_2"),
    )
)

latest = (
    windowed
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (~F.col("st.operator").isin("op_chan", "op_eve")), "left_semi")
)

result = (
    qualified
    .groupBy("moved_at", "move_type")
    .pivot("operator", ["op_dew", "op_bee", "op_eve", "op_chan"])
    .agg(F.count("unit_price"))
)
